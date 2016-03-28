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

		var gcmMessage = new gcm.Message();

		if (JSON.stringify(message.deltas).length > 3584) {
			var collectedDeltas = {new: [], updated: [], deleted: []};

			async.forEachOfSeries(message.deltas, function(deltas, key, c) {
				deltas.forEach(function(d) {
					if (JSON.stringify(collectedDeltas).length + JSON.stringify(d).length < 3584)
						collectedDeltas[key].push(d);
					else {
						gcmMessage.setDataWithObject({data: cloneObject(collectedDeltas)});

						pnSender.sendMessage(gcmMessage.toJSON(), [[message.device.persistent.token]], true, function(err, data) {
							if (err) {
								Models.Application.logger.error('Error sending GCM message: '+err.message);
							}
						});

						collectedDeltas = {new: [], updated: [], deleted: []};
						gcmMessage = new gcm.Message();
						collectedDeltas[key].push(d);
					}
				});
				c();
			}, function() {
				if (collectedDeltas.new.length || collectedDeltas.updated.length || collectedDeltas.deleted.length) {
					gcmMessage.setDataWithObject({data: cloneObject(collectedDeltas)});

					pnSender.sendMessage(gcmMessage.toJSON(), [[message.device.persistent.token]], true, function(err, data) {
						if (err) {
							Models.Application.logger.error('Error sending GCM message: '+err.message);
						}
					});
				}
			});
		} else {
			gcmMessage.setDataWithObject({data: message.deltas});
			pnSender.sendMessage(gcmMessage.toJSON(), [[message.device.persistent.token]], true, function(err, data) {
				if (err) {
					Models.Application.logger.error('Error sending GCM message: '+err.message);
				}
			});
		}
	});
};

module.exports = AndroidTransportWorker;
