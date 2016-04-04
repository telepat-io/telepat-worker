var Base_Worker = require('../base_worker');
var Models = require('telepat-models');
var async = require('async');
var apn = require('apn');
var cloneObject = require('clone');

var apnConnection = new apn.Connection({key: __dirname+'/key.pem', cert: __dirname+'/cert', passphrase: 'ficat+telepat', production: true});

/**
 *
 * @param {int} index Index of the ios transport worker. These workers consume from a common message queue
 * @constructor
 */
var iOSTransport = function(index) {
	Base_Worker.call(this, 'ios_transport', index);
};

iOSTransport.prototype = Object.create(Base_Worker.prototype);

/**
 *
 * @param {Object} message
 * @param {object} message.payload An object that has the GCM token as the key and the deltas its value
 */
iOSTransport.prototype.processMessage = function(message) {
	var key = {create: 'new', update: 'updated', delete: 'deleted'};
	var broadcast = message._broadcast;
	var apnNotification = new apn.Notification();
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
						async.each(devices.ios_transport, function(device, c) {
							payload[device.persistent.token] = message.payload['*'];
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

		async.forEachOfSeries(broadcast ? payload : message.payload, function(deltas, pnToken, c) {
			var iosDevice = new apn.Device(pnToken);

			if (JSON.stringify(deltas.deltas).length > 1920) {
				var collectedDeltas = {new: [], updated: [], deleted: []};

				async.forEach(deltas.deltas, function(d, c2) {
					if (JSON.stringify(collectedDeltas).length + JSON.stringify(d).length < 1920)
						collectedDeltas[key[d.op]].push(d);
					else {
						apnNotification.payload = {data: cloneObject(collectedDeltas)};
						apnConnection.pushNotification(apnNotification, iosDevice);

						collectedDeltas = {new: [], updated: [], deleted: []};
						apnNotification = new apn.Notification();
						apnNotification.contentAvailable = 1;
						apnNotification.sound = '';

						collectedDeltas[key[d.op]].push(d);
					}
					c2();
				}, function() {
					if (collectedDeltas.new.length || collectedDeltas.updated.length || collectedDeltas.deleted.length) {
						apnNotification.payload = {data: cloneObject(collectedDeltas)};
						apnConnection.pushNotification(apnNotification, iosDevice);
					}
				});
			} else {
				apnNotification.payload = {data: deltas};
				apnConnection.pushNotification(apnNotification, iosDevice);
			}
			c();
		});
	});
};

module.exports = iOSTransport;
