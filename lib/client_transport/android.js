var Base_Worker = require('../base_worker');
var Models = require('telepat-models');

var gcm = require('node-gcm-service');
var pnSender = new gcm.Sender({apiKey: 'AIzaSyDGnXYHBGUWih1NamoDh2MitkplqXzBFQM'});
var async = require('async');
var cloneObject = require('clone');

/**
 *
 * @param {int} index Index of the android transport worker. These workers consume from a common message queue
 * @constructor
 */
var AndroidTransportWorker = function(index) {
	Base_Worker.call(this, 'android_transport', index);
};

AndroidTransportWorker.prototype = Object.create(Base_Worker.prototype);

/**
 *
 * @param {Object} message
 * @param {object} message.payload An object that has the GCM token as the key and the deltas its value
 */
AndroidTransportWorker.prototype.processMessage = function(message) {
	var gcmMessage = new gcm.Message();
	var broadcast = !!message.payload['*'];
	var payload = {};

	async.series([
		function(callback) {
			if (broadcast) {
				Models.Subscription.getAllDevices(message.payload['*'].deltas[0].application_id, function(err, devices) {
					if (err)
						return callback(err);

					if (devices.android_transport) {
						async.each(devices.android_transport, function(token, c) {
							payload[token] = {deltas: message.payload['*'].deltas};
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
	], function(err) {
		if (err)
			return Models.Application.logger.error('Error: '+err.message);

		async.forEachOfSeries(broadcast ? payload : message.payload, function(deltas, deviceIdentifier, c) {
			var pnToken = deviceIdentifier.split('|')[1];
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
				if (JSON.stringify(notificationsPayload).length > 3968) {
					var collectedDeltas = {new: [], updated: [], deleted: []};

					async.forEachOfSeries(notificationsPayload, function(deltas2, key, c2) {
						async.forEach(deltas2, function(d, c3) {
							if (JSON.stringify(collectedDeltas).length + JSON.stringify(d).length < 3968)
								collectedDeltas[key].push(d);
							else {
								gcmMessage.setDataWithObject({data: cloneObject(collectedDeltas)});

								pnSender.sendMessage(gcmMessage.toJSON(), [[pnToken]], true, function(err) {
									if (err) {
										Models.Application.logger.error('Error sending GCM message: '+err.message);
									}
								});

								collectedDeltas = {new: [], updated: [], deleted: []};
								gcmMessage = new gcm.Message();
								collectedDeltas[key].push(d);
							}
							c3();
						}, function() {
							if (collectedDeltas.new.length || collectedDeltas.updated.length || collectedDeltas.deleted.length) {
								gcmMessage.setDataWithObject({data: cloneObject(collectedDeltas)});

								pnSender.sendMessage(gcmMessage.toJSON(), [[pnToken]], true, function(err) {
									if (err) {
										Models.Application.logger.error('Error sending GCM message: '+err.message);
									}
								});
							}
							c2();
						});
					});
				} else {
					gcmMessage.setDataWithObject({data: notificationsPayload});
					pnSender.sendMessage(gcmMessage.toJSON(), [[pnToken]], true, function(err) {
						if (err) {
							Models.Application.logger.error('Error sending GCM message: '+err.message);
						}
					});
				}
				c();
			});
		});
	});
};

module.exports = AndroidTransportWorker;
