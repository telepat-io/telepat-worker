var Base_Worker = require('../base_worker');
var async = require('async');
var apn = require('apn');
var cloneObject = require('clone');

var apnConnection = new apn.Connection({key: __dirname+'/key.pem', cert: __dirname+'/cert', passphrase: 'ficat+telepat', production: true});

var iOSTransport = function(index) {
	Base_Worker.call(this, 'ios_transport', index);
};

iOSTransport.prototype = Object.create(Base_Worker.prototype);

iOSTransport.prototype.processMessage = function(message) {
	var iosDevice = new apn.Device(message.device.persistent.token);
	var key = {create: 'new', update: 'updated', delete: 'deleted'};
	var apnNotification = new apn.Notification();
	apnNotification.contentAvailable = 1;
	apnNotification.sound = '';

	async.forEachOfSeries(message.payload, function(deltas, pnToken, c) {
		if (JSON.stringify(deltas.deltas).length > 1536) {
			var collectedDeltas = {new: [], updated: [], deleted: []};

			async.forEach(deltas.deltas, function(d, c2) {
				if (JSON.stringify(collectedDeltas).length + JSON.stringify(d).length < 1536)
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
			apnNotification.payload = {data: message.deltas};
			apnConnection.pushNotification(apnNotification, iosDevice);
		}
		c();
	});
};

module.exports = iOSTransport;
