var Base_Worker = require('../base_worker');
var tlib = require('telepat-models');
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

iOSTransport.prototype.removeInvalidDevices = function(tokens) {
	async.each(tokens, function(token, c) {
		var dev = cloneObject(collectedDeviceIds[token]);
		delete collectedDeviceIds[token];

		if (dev) {
			async.series([
				function(callback) {
					tlib.subscription.removeAllSubscriptionsFromDevice(dev.app, dev.id, token, 'ios_transport', callback);
				},
				function(callback) {
					tlib.subscription.removeDevice(dev.app, dev.id, callback);
				}
			], function(err) {
				if (err) {
					return tlib.services.logger.error('Error removing device and/or subscriptions: ' + err.message);
				}

				tlib.services.logger.debug('Device ' + JSON.stringify({id: dev.id, token: token}) + ' removed due to permanent failure from APN');
			});
		}

		c();
	});
};

/**
 *
 * @param {string} applicationId Application ID used to determine which apn connection to use
 * @param {Notification} apnNotification Notification instantiated from the apn module
 * @param {string[]} iosTokens Array of APN tokens to send the message to
 */
iOSTransport.prototype.sendNotification = function(applicationId, apnNotification, iosTokens) {
	var app = tlib.apps[applicationId],
		self = this;

	if (!app) {
		return tlib.services.logger.error('Application with ID ' + applicationId + ' not found, unable to send notification(s)');
	}

	if (!app.isAPNConfigured()) {
		return tlib.services.logger.error('Application '+applicationId+' is not configured for APN');
	}

	if (!apnConnections[applicationId]) {
		apnConnections[applicationId] = new apn.Provider({
			token: {
				key: app.apn_key,
				keyId: app.apn_key_id,
				teamId: app.apn_team_id
			},
			production: false
		});
		apnConnections[applicationId]._clientOptions = {
			token: {
				key: app.apn_key,
				keyId: app.apn_key_id,
				teamId: app.apn_team_id
			},
			production: false
		};
	} else {
		var hasChanged = (app.apn_key != apnConnections[applicationId]._clientOptions.token.key &&
			app.apn_key_id != apnConnections[applicationId]._clientOptions.token.keyId &&
			app.apn_team_id != apnConnections[applicationId]._clientOptions.token.teamId);

		if (hasChanged) {
			delete apnConnections[applicationId];

			apnConnections[applicationId] = new apn.Provider({
				token: {
					key: app.apn_key,
					keyId: app.apn_key_id,
					teamId: app.apn_team_id
				},
				production: false
			});
			apnConnections[applicationId]._clientOptions = {
				token: {
					key: app.apn_key,
					keyId: app.apn_key_id,
					teamId: app.apn_team_id
				},
				production: false
			};
		}
	}

	apnNotification.topic = app.apn_topic;
	apnConnections[applicationId].send(apnNotification, iosTokens).then(function (response) {
		if (response.failed.length) {
			var badTokenDevices = [];

			response.failed.forEach(function(device) {
				var token = device.device;

				if (device.response && device.response.reason === 'BadDeviceToken' && collectedDeviceIds[token]) {
					badTokenDevices.push(token);
				} else if (device.response) {
					tlib.services.logger.warning('Failed sending notification to device '
						+ JSON.stringify({id: collectedDeviceIds[token].id, token: token}) + ': ' + device.response.reason);
				}
			});

			self.removeInvalidDevices(badTokenDevices);
		}
		if (response.sent.length) {
			tlib.services.logger.debug('Sent ' + response.sent.length + ' notifications');
		}
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

				tlib.subscription.getAllDevices(message.payload['*'].applicationId, function(err, deviceIdentifiers) {
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
			return tlib.services.logger.error('Error: '+err.message);

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
							var iosPushProperty = mdl ? tlib.apps[applicationId].modelSchema(mdl)
							&& tlib.apps[applicationId].modelSchema(mdl).ios_push_field : null;

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
										if (err) return tlib.services.logger.error(err.message);

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
						var iosPushProperty = mdl ? tlib.apps[applicationId].modelSchema(mdl)
							&& tlib.apps[applicationId].modelSchema(mdl).ios_push_field : null;;

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