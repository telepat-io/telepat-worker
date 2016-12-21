var Base_Worker = require('../base_worker');
var Models = require('telepat-models');
var async = require('async');
var apn = require('apn');
var cloneObject = require('clone');

var apnConnections = {};
var collectedDeviceIds = {};

/**
 *
 * @param {int} index Index of the ios transport worker. These workers consume from a common message queue
 * @constructor
 */
var iOSTransport = function(index, config) {
	Base_Worker.call(this, 'ios_transport', index, config);
};

iOSTransport.prototype = Object.create(Base_Worker.prototype);

iOSTransport.prototype.onInvalidToken = function(errCode, notification, device) {
	if (errCode == 8) {
		var dev = cloneObject(collectedDeviceIds[device.token.toString('hex')]);
		delete collectedDeviceIds[device.token.toString('hex')];

		if (dev) {
			async.series([
				function(callback) {
					Models.Subscription.removeAllSubscriptionsFromDevice(dev.app, dev.id, callback);
				},
				function(callback) {
					Models.Subscription.removeDevice(dev.app, dev.id, callback);
				}
			], function(err) {
				if (err) {
					return Models.Application.logger.error('Error removing device and/or subscriptions: ' + err);
				}

				Models.Application.logger.debug('Device ' + JSON.stringify({id: dev.id, token: device.token.toString('hex')}) + ' removed due to permanent failure from APN');
			});
		} else {
			Models.Application.logger.alert('Failed removing device: not present in object');
		}
	}
};

/**
 *
 * @param {string} applicationId Application ID used to determine which apn connection to use
 * @param {Notification} apnNotification Notification instantiated from the apn module
 * @param {string[]} iosDevices Array of APN tokens to send the message to
 */
iOSTransport.prototype.sendNotification = function(applicationId, apnNotification, iosDevices) {
	var app = Models.Application.loadedAppModels[applicationId];

	if (!app) {
		return Models.Application.logger.error('Application with ID ' + applicationId + ' not found, unable to send notification(s)');
	}

	if ((!app.apn_key && !app.apn_cert && !app.apn_passphrase)) {
		if (!app.apn_pfx)
			return Models.Application.logger.error('Application '+applicationId+' is not configured for APN');
	}

	if (!apnConnections[applicationId]) {
		if (!app.apn_pfx){
			apnConnections[applicationId] = new apn.Connection({key: app.apn_key, cert: app.apn_cert, passphrase: app.apn_passphrase, production: true});
		} else	{
			apnConnections[applicationId] = new apn.Connection({pfx: new Buffer(app.apn_pfx, 'base64'), passphrase: app.apn_passphrase, production: true});
		}

		apnConnections[applicationId].on('transmissionError', this.onInvalidToken);
	} else {
		var hasChanged = null;

		if (apnConnections[applicationId].options.key && apnConnections[applicationId].options.cert)
			hasChanged = (app.apn_key != apnConnections[applicationId].options.key &&
				app.apn_cert != apnConnections[applicationId].options.cert);
		else if (apnConnections[applicationId].options.pfx)
			hasChanged = app.apn_pfx != apnConnections[applicationId].options.pfx.toString('base64');

		hasChanged =  hasChanged || apnConnections[applicationId].options.passphrase != app.apn_passphrase;

		if (hasChanged) {
			delete apnConnections[applicationId];

			if (!app.apn_pfx){
				apnConnections[applicationId] = new apn.Connection({key: app.apn_key, cert: app.apn_cert, passphrase: app.apn_passphrase, production: true});
			} else	{
				apnConnections[applicationId] = new apn.Connection({pfx: new Buffer(app.apn_pfx, 'base64'), passphrase: app.apn_passphrase, production: true});
			}

			apnConnections[applicationId].on('transmissionError', this.onInvalidToken);
		}
	}

	async.each(iosDevices, function(device, c) {
		apnConnections[applicationId].pushNotification(apnNotification, device);
		c();
	});
};

/**
 *
 * @param {ClientTransportMessage} message
 */
iOSTransport.prototype.processMessage = function(message) {
	var broadcast = !!message.payload['*'];
	var payload = {'*': {deviceTokens: [], deltas: [], applicationId: null}};
	var self = this;

	var apnNotification = new apn.Notification();
	apnNotification.contentAvailable = 1;
	apnNotification.sound = '';

	async.series([
		function(callback) {
			if (broadcast) {
				payload['*'].applicationId = message.payload['*'].applicationId;
				payload['*'].deltas = message.payload['*'].deltas;

				Models.Subscription.getAllDevices(message.payload['*'].applicationId, function(err, deviceIdentifiers) {
					if (err) {
						return callback(err);
					}

					if (deviceIdentifiers.ios_transport) {
						payload['*'].deviceTokens = deviceIdentifiers.ios_transport;
					}

					callback();
				});
			} else {
				setImmediate(callback);
			}
		}
	], function (err) {
		if (err)
			return Models.Application.logger.error('Error: '+err.message);

		async.forEach(broadcast ? payload : message.payload, function(groupedSubscription, c) {
			var applicationId = groupedSubscription.applicationId,
				notificationsPayload = {new: [], updated: [], deleted: []};

			var deviceTokens = [];

			groupedSubscription.deviceTokens.forEach(function(device) {
				// "deviceId|apnToken"
				var splitString = device.split('|');
				deviceTokens.push(splitString[1]);
				collectedDeviceIds[splitString[1]] = {id: splitString[0], app: groupedSubscription.applicationId};
			});

			async.eachSeries(groupedSubscription.deltas, function(d, c2) {
				if (d.op == 'create')
					notificationsPayload.new.push(d);
				else if (d.op == 'update')
					notificationsPayload.updated.push(d);
				else if (d.op == 'delete')
					notificationsPayload.deleted.push(d);
				c2();
			}, function() {
				if (JSON.stringify(notificationsPayload).length > 1920) {
					var collectedDeltas = {new: [], updated: [], deleted: []};

					async.forEachOfSeries(notificationsPayload, function(deltas2, key, c2) {
						async.forEach(deltas2, function(d, c3) {
							var mdl = d.object.type;
							var iosPushProperty = mdl ? Models.Application.loadedAppModels[applicationId].schema[mdl]
							&& Models.Application.loadedAppModels[applicationId].schema[mdl].ios_push_field : null;

							if (key == 'new' && iosPushProperty && d.object[iosPushProperty]) {
								var specialNotification = new apn.Notification();
								specialNotification.sound = 'default';
								specialNotification.alert = d.object[iosPushProperty];

								self.sendNotification(applicationId, specialNotification, deviceTokens);
							} else if (JSON.stringify(collectedDeltas).length + JSON.stringify(d).length < 1920)
								collectedDeltas[key].push(d);
							else {

								if (collectedDeltas.new.length + collectedDeltas.updated.length + collectedDeltas.deleted.length > 1)	{
									apnNotification.payload = {data: cloneObject(collectedDeltas)};
									self.sendNotification(applicationId, apnNotification, deviceTokens);
								} else {
									if (!collectedDeltas[key].length)
										collectedDeltas[key].push(d);

									self.storeAmazonLink(cloneObject(collectedDeltas), function(err, resourcePublicLink) {
										if (err) return Models.Application.logger.error(err.message);

										apnNotification.payload = {url: resourcePublicLink};
										self.sendNotification(applicationId, apnNotification, deviceTokens);
									});
								}

								collectedDeltas = {new: [], updated: [], deleted: []};
								apnNotification = new apn.Notification();
								apnNotification.contentAvailable = 1;
								apnNotification.sound = '';

								collectedDeltas[key].push(d);
							}
							c3();
						}, function() {
							if (collectedDeltas.new.length || collectedDeltas.updated.length || collectedDeltas.deleted.length) {
								apnNotification.payload = {data: cloneObject(collectedDeltas)};
								self.sendNotification(applicationId, apnNotification, deviceTokens);
							}
							c2();
						});
					});
				} else {
					//filter new deltas that have to be sent through push notifications (rather than remote notif)
					notificationsPayload.new = notificationsPayload.new.map(function(delta) {
						var mdl = delta.object.type;
						var iosPushProperty = mdl ? Models.Application.loadedAppModels[applicationId].schema[mdl]
						&& Models.Application.loadedAppModels[applicationId].schema[mdl].ios_push_field : null;

						if (iosPushProperty && delta.object[iosPushProperty]) {
							var specialNotification = new apn.Notification();
							specialNotification.sound = 'default';
							specialNotification.alert = delta.object[iosPushProperty];

							self.sendNotification(applicationId, specialNotification, deviceTokens);
						} else
							return delta;
					});

					notificationsPayload.new = notificationsPayload.new.filter(function(d) {
						return d;
					});

					if (notificationsPayload.new.length || notificationsPayload.updated.length || notificationsPayload.deleted.length) {
						apnNotification.payload = {data: notificationsPayload};
						self.sendNotification(applicationId, apnNotification, deviceTokens);
					}
				}
				c();
			});
		});
	});
};

module.exports = iOSTransport;
