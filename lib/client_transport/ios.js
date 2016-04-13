var Base_Worker = require('../base_worker');
var Models = require('telepat-models');
var async = require('async');
var apn = require('apn');
var cloneObject = require('clone');

var apnConnections = {};

var iOSTransport = function(index) {
	Base_Worker.call(this, 'ios_transport', index);
};

iOSTransport.prototype = Object.create(Base_Worker.prototype);

iOSTransport.prototype.sendNotification = function(application_id, apnNotification, iosDevice) {
	Models.Application(application_id, function(err, app) {
		if (err)
			return Models.Application.logger.error('Error sending notification: '+err);

		if ((!app.apn_key && !app.apn_cert && !app.apn_passphrase)) {
			if (!app.apn_pfx)
				return Models.Application.logger.error('Application '+application_id+' is not configured for APN');
		}


		if (!apnConnections[application_id]) {
			if (!app.apn_pfx)
				apnConnections[application_id] = new apn.Connection({key: app.apn_key, cert: app.apn_cert, passphrase: app.apn_passphrase, production: true});
			else
				apnConnections[application_id] = new apn.Connection({pfx: new Buffer(app.apn_pfx, 'base64'), passphrase: app.apn_passphrase, production: true});
		} else {
			if (app.apn_key != apnConnections[application_id].options.key ||
				app.apn_cert != apnConnections[application_id].options.cert ||
				app.apn_passphrase != apnConnections[application_id].options.passphrase ||
				app.apn_pfx != apnConnections[application_id].options.pfx) {

				delete apnConnections[application_id];
				if (!app.apn_pfx)
					apnConnections[application_id] = new apn.Connection({key: app.apn_key, cert: app.apn_cert, passphrase: app.apn_passphrase, production: true});
				else
					apnConnections[application_id] = new apn.Connection({pfx: new Buffer(app.apn_pfx, 'base64'), passphrase: app.apn_passphrase, production: true});
			}
		}

		apnConnections[application_id].pushNotification(apnNotification, iosDevice);
	});
};

iOSTransport.prototype.processMessage = function(message) {
	var objectTimestamps = {}; //so we don't check the same object from different subscriptions
	var self = this;

	async.filter(message.deltas.updated, function(patch, filterCallback) {
		var transportMessageTimestamp = patch._microtime;
		var pathParts = patch.path.split('/');
		delete patch._microtime;

		if (objectTimestamps[patch.path])
			return filterCallback(true);

		objectTimestamps[patch.path] = true;

		var transportMessageKey = 'blg:'+patch.applicationId+':'+pathParts.join(':')+':transport_msg_timestamp';
		Models.Application.redisClient.get(transportMessageKey, function(err, result) {
			if (result) {
				if (parseInt(result) <= transportMessageTimestamp)
					return filterCallback(true);
				else
					return filterCallback(false);
			} else {
				filterCallback(true);
			}
		});
	}, function(results) {
		message.deltas.updated = results;

		if (message.device.persistent && message.device.persistent.token) {
			var iosDevice = new apn.Device(message.device.persistent.token);

			var apnNotification = new apn.Notification();
			apnNotification.contentAvailable = 1;
			apnNotification.sound = '';

			if (JSON.stringify(message.deltas).length > 1536) {
				var collectedDeltas = {new: [], updated: [], deleted: []};

				async.forEachOfSeries(message.deltas, function(deltas, key, c) {
					deltas.forEach(function(d) {
						var mdl = d.value.type;
						var iosPushProperty = mdl ? Models.Application.loadedAppModels[d.value.application_id].schema[mdl]
						&& Models.Application.loadedAppModels[d.value.application_id].schema[mdl].ios_push_field : null;

						if (key == 'new' && iosPushProperty && d.value[iosPushProperty]) {
							var specialNotification = new apn.Notification();
							specialNotification.sound = 'default';
							specialNotification.alert = d.value[iosPushProperty];

							self.sendNotification(d.value.application_id, specialNotification, iosDevice);
						} else if (JSON.stringify(collectedDeltas).length + JSON.stringify(d).length < 1536)
							collectedDeltas[key].push(d);
						else {
							apnNotification.payload = {data: cloneObject(collectedDeltas)};
							self.sendNotification(message.device.application_id, apnNotification, iosDevice);

							collectedDeltas = {new: [], updated: [], deleted: []};
							apnNotification = new apn.Notification();
							apnNotification.contentAvailable = 1;
							apnNotification.sound = '';

							collectedDeltas[key].push(d);
						}
					});
					c();
				}, function() {
					if (collectedDeltas.new.length || collectedDeltas.updated.length || collectedDeltas.deleted.length) {
						apnNotification.payload = {data: cloneObject(collectedDeltas)};
						self.sendNotification(message.device.application_id, apnNotification, iosDevice);
					}
				});
			} else {
				//filter new deltas that have to be sent through push notifications (rather than remote notif)
				message.deltas.new = message.deltas.new.map(function(delta) {
					var mdl = delta.value.type;
					var iosPushProperty = mdl ? Models.Application.loadedAppModels[delta.value.application_id].schema[mdl]
					&& Models.Application.loadedAppModels[delta.value.application_id].schema[mdl].ios_push_field : null;

					if (iosPushProperty && delta.value[iosPushProperty]) {
						var specialNotification = new apn.Notification();
						specialNotification.sound = 'default';
						specialNotification.alert = delta.value[iosPushProperty];

						self.sendNotification(delta.value.application_id, specialNotification, iosDevice);
					} else
						return delta;
				});

				apnNotification.payload = {data: message.deltas};
				self.sendNotification(message.device.application_id, apnNotification, iosDevice);
			}
		} else {
			Models.Application.logger.warning('Device '+message.device.id+' is missing push notification token');
		}
	});
};

module.exports = iOSTransport;
