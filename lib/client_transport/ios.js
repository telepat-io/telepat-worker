var Base_Worker = require('../base_worker');
var Models = require('telepat-models');
var async = require('async');
var apn = require('apn');

var apnConnection = new apn.Connection({key: './lib/client_transport/key.pem', cert: './lib/client_transport/cert', passphrase: 'ficat+telepat'});

var iOSTransport = function(index) {
	Base_Worker.call(this, 'ios_transport', index);
};

iOSTransport.prototype = new Base_Worker();

iOSTransport.prototype.processMessage = function(message) {
	var apnNotification = new apn.Notification();

	var objectTimestamps = {}; //so we don't check the same object from different subscriptions

	async.filter(message.deltas.updated, function(patch, filterCallback) {
		var transportMessageTimestamp = patch._microtime;
		var pathParts = patch.path.split('/');
		delete patch._microtime;

		if (objectTimestamps[patch.path])
			return filterCallback(true);

		objectTimestamps[patch.path] = true;

		var transportMessageKey = 'blg:'+message.applicationId+':'+pathParts.join(':')+':transport_msg_timestamp';
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
			apnNotification.payload = {data: message.deltas};
			apnNotification['content-available'] = 1;

			apnConnection.pushNotification(apnNotification, iosDevice);
		} else {
			console.log('Device '+message.device.id+' is missing push notification token');
		}
	});
};

module.exports = iOSTransport;
