var Base_Worker = require('../base_worker');
var Models = require('telepat-models');

var gcm = require('node-gcm-service');
var pnSender = new gcm.Sender({apiKey: 'AIzaSyDGnXYHBGUWih1NamoDh2MitkplqXzBFQM'});
var async = require('async');
var cloneObject = require('clone');

var AndroidTransportWorker = function(index) {
	Base_Worker.call(this, 'android_transport', index);
};

AndroidTransportWorker.prototype = Object.create(Base_Worker.prototype);

AndroidTransportWorker.prototype.processMessage = function(message) {
	var gcmMessage = new gcm.Message();
	var key = {create: 'new', update: 'updated', delete: 'deleted'};

	async.forEachOfSeries(message.payload, function(deltas, pnToken, c) {
		if (JSON.stringify(deltas.deltas).length > 3584) {
			var collectedDeltas = {new: [], updated: [], deleted: []};

			async.forEach(deltas.deltas, function(d, c2) {
				if (JSON.stringify(collectedDeltas).length + JSON.stringify(d).length < 3584)
					collectedDeltas[key[d.op]].push(d);
				else {
					gcmMessage.setDataWithObject({data: cloneObject(collectedDeltas)});

					console.log('Sending chunked', JSON.stringify({data: cloneObject(collectedDeltas)}, null, 2));
					/*pnSender.sendMessage(gcmMessage.toJSON(), [[pnToken]], true, function(err, data) {
						if (err) {
							Models.Application.logger.error('Error sending GCM message: '+err.message);
						}
					});*/

					collectedDeltas = {new: [], updated: [], deleted: []};
					gcmMessage = new gcm.Message();
					collectedDeltas[key[d.op]].push(d);
				}
				c2();
			}, function() {
				if (collectedDeltas.new.length || collectedDeltas.updated.length || collectedDeltas.deleted.length) {
					gcmMessage.setDataWithObject({data: cloneObject(collectedDeltas)});

					console.log('Sending chunked', JSON.stringify({data: cloneObject(collectedDeltas)}, null, 2));
					/*pnSender.sendMessage(gcmMessage.toJSON(), [[message.device.persistent.token]], true, function(err, data) {
						if (err) {
							Models.Application.logger.error('Error sending GCM message: '+err.message);
						}
					});*/
				}
			});
		} else {
			gcmMessage.setDataWithObject({data: message.deltas});
			console.log('Sending whole', JSON.stringify({data: message.deltas}, null, 2));
			/*pnSender.sendMessage(gcmMessage.toJSON(), [[pnToken]], true, function(err, data) {
				if (err) {
					Models.Application.logger.error('Error sending GCM message: '+err.message);
				}
			});*/
		}
		c();
	});
};

module.exports = AndroidTransportWorker;
