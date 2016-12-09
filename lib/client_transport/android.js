var Base_Worker = require('../base_worker');
var Models = require('telepat-models');

var gcm = require('node-gcm-service');
var pnSender = new gcm.Sender({apiKey: 'AIzaSyDGnXYHBGUWih1NamoDh2MitkplqXzBFQM'});
var async = require('async');
var cloneObject = require('clone');

var gcmConnections = {};

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
 * @param {string} application_id
 * @param {gcm.Message} notification
 * @param {string} pnToken
 */
AndroidTransportWorker.prototype.sendNotification = function(application_id, notification, pnToken, callback) {
	Models.Application(application_id, function(err, app) {
		if (err)
			return Models.Application.logger.error('Error sending notification: '+err);

		if (!app.gcm_api_key) {
			return Models.Application.logger.error('Application '+application_id+' is not configured for GCM');
		}

		if (!gcmConnections[application_id]) {
			gcmConnections[application_id] = new gcm.Sender({apiKey: app.gcm_api_key});
		} else {
			if (app.gcm_api_key != gcmConnections[application_id].api_key) {
				delete gcmConnections[application_id];

				gcmConnections[application_id] = new gcm.Sender({apiKey: app.gcm_api_key});
			}
		}

		gcmConnections[application_id].sendMessage(notification.toJSON(), [[pnToken]], true, callback);
	});
};

/**
 *
 * @param {Object} message
 * @param {object} message.payload An object that has the GCM token as the key and the deltas its value
 */
AndroidTransportWorker.prototype.processMessage = function(message) {
	var gcmMessage = new gcm.Message();
	var broadcast = !!message.payload['*'];
	var payload = {};
	var self = this;

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
			var deviceId = deviceIdentifier.split('|')[0];
			var application_id = deltas.application_id || message.payload['*'].deltas[0].application_id;
			var notificationsPayload = {new: [], updated: [], deleted: []};

			var sendCallback = function(err, data) {
				if (err) {
					Models.Application.logger.error('Error sending GCM message: '+err.message);
				} else {
					if (data.failures_length) {
						async.series([
							function(callback) {
								Models.Subscription.removeAllSubscriptionsFromDevice(application_id, deviceId, callback);
							},
							function(callback) {
								Models.Subscription.removeDevice(application_id, deviceId, callback);
							}
						], function(err) {
							if (err)
								return Models.Application.logger.error(err.message);
						});
					} else if (data.canonical_ids_length) {
						var subscriptions = [];
						async.series([
							function(callback) {
								Models.Subscription.removeAllSubscriptionsFromDevice(application_id, deviceId, function(err, results) {
									if (err)
										return Models.Application.logger.error(err.message);

									subscriptions = results;
									callback();
								});
							},
							function(callback) {
								Models.Subscription.updateDevice(application_id, deviceId, {
									persistent: {
										active: 1,
										token: data.canonical_ids[0].new_registration_id,
										type: "android"
									}
								}, callback);
							},
							function(callback) {
								if (!subscriptions.length)
									return callback();

								var tranzaction = Models.Application.redisClient.multi();

								subscriptions.forEach(function(sub) {
									tranzaction.sadd([sub, 'android_transport|'+deviceId+'|'+data.canonical_ids[0].new_registration_id+'|'+application_id]);
								});

								tranzaction.exec(callback);
							}
						], function(err) {
							if (err)
								return Models.Application.logger.error(err.message);
						});
					}
				}
			};

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

								self.sendNotification(application_id, gcmMessage, pnToken, sendCallback);

								collectedDeltas = {new: [], updated: [], deleted: []};
								gcmMessage = new gcm.Message();
								collectedDeltas[key].push(d);
							}
							c3();
						}, function() {
							if (collectedDeltas.new.length || collectedDeltas.updated.length || collectedDeltas.deleted.length) {
								gcmMessage.setDataWithObject({data: cloneObject(collectedDeltas)});

								self.sendNotification(application_id, gcmMessage, pnToken, sendCallback);
							}
							c2();
						});
					});
				} else {
					gcmMessage.setDataWithObject({data: notificationsPayload});
					self.sendNotification(application_id, gcmMessage, pnToken, sendCallback);
				}
				c();
			});
		});
	});
};

module.exports = AndroidTransportWorker;
