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
var iOSTransport = function(index) {
	Base_Worker.call(this, 'ios_transport', index);
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
				if (err) console.log(err);
			});
		}
	}
};

iOSTransport.prototype.sendNotification = function(application_id, apnNotification, iosDevice) {
	var self = this;

	Models.Application(application_id, function(err, app) {
		if (err)
			return Models.Application.logger.error('Error sending notification: '+err);

		if ((!app.apn_key && !app.apn_cert && !app.apn_passphrase)) {
			if (!app.apn_pfx)
				return Models.Application.logger.error('Application '+application_id+' is not configured for APN');
		}

		if (!apnConnections[application_id]) {
			if (!app.apn_pfx){
				apnConnections[application_id] = new apn.Connection({key: app.apn_key, cert: app.apn_cert, passphrase: app.apn_passphrase, production: true});
			} else	{
				apnConnections[application_id] = new apn.Connection({pfx: new Buffer(app.apn_pfx, 'base64'), passphrase: app.apn_passphrase, production: true});
			}

			apnConnections[application_id].on('transmissionError', self.onInvalidToken);
		} else {
			var hasChanged = null;

			if (apnConnections[application_id].options.key && apnConnections[application_id].options.cert)
				hasChanged = (app.apn_key != apnConnections[application_id].options.key &&
					app.apn_cert != apnConnections[application_id].options.cert);
			else if (apnConnections[application_id].options.pfx)
				hasChanged = app.apn_pfx != apnConnections[application_id].options.pfx.toString('base64');

			hasChanged =  hasChanged || apnConnections[application_id].options.passphrase != app.apn_passphrase;

			if (hasChanged) {
				delete apnConnections[application_id];

				if (!app.apn_pfx){
					apnConnections[application_id] = new apn.Connection({key: app.apn_key, cert: app.apn_cert, passphrase: app.apn_passphrase, production: true});
				} else	{
					apnConnections[application_id] = new apn.Connection({pfx: new Buffer(app.apn_pfx, 'base64'), passphrase: app.apn_passphrase, production: true});
				}

				apnConnections[application_id].on('transmissionError', self.onInvalidToken);
			}
		}

		apnConnections[application_id].pushNotification(apnNotification, iosDevice);
	});
};

/**
 *
 * @param {Object} message
 * @param {object} message.payload An object that has the GCM token as the key and the deltas its value
 * @param {bool} message._broadcast
 * @param {string} message.application_id
 */
iOSTransport.prototype.processMessage = function(message) {
	var broadcast = !!message.payload['*'];
	var apnNotification = new apn.Notification();
	var self = this;
	apnNotification.contentAvailable = 1;
	apnNotification.sound = '';

	var payload = {};

	async.series([
		function(callback) {
			if (broadcast) {
				Models.Subscription.getAllDevices(message.payload['*'].deltas[0].application_id, function(err, devices) {
					if (err)
						return callback(err);

					if (devices.ios_transport) {
						async.each(devices.ios_transport, function(deviceToken, c) {
							payload[deviceToken] = {deltas: message.payload['*'].deltas};
							c();
						}, callback);
					} else {
						callback();
					}
				});
			} else {
				callback();
			}
		}
	], function (err) {
		if (err)
			return Models.Application.logger.error('Error: '+err.message);

		async.forEachOfSeries(broadcast ? payload : message.payload, function(deltas, deviceIdentifier, c) {
			var pnToken = deviceIdentifier.split('|')[1];
			var iosDevice = new apn.Device(pnToken);
			var application_id = deltas.application_id || message.payload['*'].deltas[0].application_id;
			collectedDeviceIds[pnToken] = {id: deviceIdentifier.split('|')[0], app: application_id};

			var notificationsPayload = {new: [], updated: [], deleted: []};

			async.eachSeries(deltas.deltas, function(d, c2) {
				if (d.op == 'create')
					notificationsPayload.new.push(d);
				else if (d.op == 'update')
					notificationsPayload.updated.push(d);
				else if (d.op == 'delete')
					notificationsPayload.updated.push(d);
				c2();
			}, function() {
				if (JSON.stringify(notificationsPayload).length > 1920) {
					var collectedDeltas = {new: [], updated: [], deleted: []};

					async.forEachOfSeries(notificationsPayload, function(deltas2, key, c2) {
						async.forEach(deltas2, function(d, c3) {
							var mdl = d.object.type;
							var iosPushProperty = mdl ? Models.Application.loadedAppModels[application_id].schema[mdl]
							&& Models.Application.loadedAppModels[application_id].schema[mdl].ios_push_field : null;

							if (key == 'new' && iosPushProperty && d.object[iosPushProperty]) {
								var specialNotification = new apn.Notification();
								specialNotification.sound = 'default';
								specialNotification.alert = d.object[iosPushProperty];

								self.sendNotification(application_id, specialNotification, iosDevice);
							} else if (JSON.stringify(collectedDeltas).length + JSON.stringify(d).length < 1920)
								collectedDeltas[key].push(d);
							else {
								apnNotification.payload = {data: cloneObject(collectedDeltas)};
								self.sendNotification(application_id, apnNotification, iosDevice);

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
								self.sendNotification(application_id, apnNotification, iosDevice);
							}
							c2();
						});
					});
				} else {
					//filter new deltas that have to be sent through push notifications (rather than remote notif)
					notificationsPayload.new = notificationsPayload.new.map(function(delta) {
						var mdl = delta.object.type;
						var iosPushProperty = mdl ? Models.Application.loadedAppModels[application_id].schema[mdl]
						&& Models.Application.loadedAppModels[application_id].schema[mdl].ios_push_field : null;

						if (iosPushProperty && delta.object[iosPushProperty]) {
							var specialNotification = new apn.Notification();
							specialNotification.sound = 'default';
							specialNotification.alert = delta.object[iosPushProperty];

							self.sendNotification(application_id, specialNotification, iosDevice);
						} else
							return delta;
					});

					apnNotification.payload = {data: notificationsPayload};
					self.sendNotification(application_id, apnNotification, iosDevice);
				}
				c();
			});
		});
	});
};

module.exports = iOSTransport;
