var Base_Worker = require('../base_worker');
var Models = require('telepat-models');
var async = require('async');
var apn = require('apn');
var cloneObject = require('clone');

var apnConnection = new apn.Connection({key: __dirname+'/key.pem', cert: __dirname+'/cert', passphrase: 'ficat+telepat', production: true});

var iOSTransport = function(index) {
	Base_Worker.call(this, 'ios_transport', index);
};

iOSTransport.prototype = Object.create(Base_Worker.prototype);

iOSTransport.prototype.processMessage = function(message) {
	var objectTimestamps = {}; //so we don't check the same object from different subscriptions

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
						if (JSON.stringify(collectedDeltas).length + JSON.stringify(d).length < 1536)
							collectedDeltas[key].push(d);
						else {
							apnNotification.payload = {data: cloneObject(collectedDeltas)};
							apnConnection.pushNotification(apnNotification, iosDevice);

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
						apnConnection.pushNotification(apnNotification, iosDevice);
					}
				});
			} else {
				apnNotification.payload = {data: message.deltas};
				apnConnection.pushNotification(apnNotification, iosDevice);
			}
		} else {
			Models.Application.logger.warning('Device '+message.device.id+' is missing push notification token');
		}
	});
};

module.exports = iOSTransport;
