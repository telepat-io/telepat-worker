var Base_Worker = require('../base_worker');
var tlib = require('telepat-models');

var gcm = require('node-gcm');
var async = require('async');
var cloneObject = require('clone');

var gcmConnections = {};

/**
 *
 * @param {int} index Index of the android transport worker. These workers consume from a common message queue
 * @constructor
 */
var AndroidTransportWorker = function(index, config) {
	Base_Worker.call(this, 'android_transport', index, config);
};

AndroidTransportWorker.prototype = Object.create(Base_Worker.prototype);

/**
 *
 * @param {string} applicationId
 * @param {gcm.Message} notification
 * @param {string[]} pnTokens
 * @param {Function} responseCallback
 */
AndroidTransportWorker.prototype.sendNotification = function(applicationId, notification, pnTokens, responseCallback) {
	var app = tlib.apps[applicationId];

	if (!app) {
		return tlib.services.logger.error('Application with ID ' + applicationId + ' not found, unable to send notification(s)');
	}

	if (!app.isGCMConfigured()) {
		return tlib.services.logger.error('Application '+applicationId+' is not configured for GCM');
	}

	pnTokens = pnTokens.map(function(device) {
		return device.split('|')[1];
	});

	if (!gcmConnections[applicationId]) {
		gcmConnections[applicationId] = new gcm.Sender(app.gcm_api_key);
	} else {
		if (app.gcm_api_key != gcmConnections[applicationId].api_key) {
			delete gcmConnections[applicationId];

			gcmConnections[applicationId] = new gcm.Sender(app.gcm_api_key);
		}
	}

	for(var i = 0; i < pnTokens.length; i += 999) {
		gcmConnections[applicationId].send(notification, {registrationTokens: pnTokens.slice(i, i+999)}, responseCallback);
	}
};

AndroidTransportWorker.sendCallback = function(devices, applicationId) {
	var self = this;

	return function(err, data) {
		var failures = [], canonical = [];

		data.results.forEach(function(result, index) {
			if (result.error) {
				failures.push(devices[index]);
			} else if (result.registration_id) {
				var newDevice = cloneObject(devices[index]);
				newDevice.newToken = result.registration_id;
				canonical.push(newDevice);
			}
		});

		if (err) {
			tlib.services.logger.error('Error sending GCM message: ' + err.details);
		} else {
			if (failures) {
				async.forEach(failures, function(failedDevice, c2) {
					async.series([
						function(callback) {
							tlib.subscription.removeAllSubscriptionsFromDevice(applicationId, failedDevice.id, failedDevice.token, 'android_transport', callback);
						},
						function(callback) {
							tlib.subscription.removeDevice(applicationId, failedDevice.id, callback);
						}
					], function(err) {
						if (err)
							return tlib.services.logger.error(err.message);

						tlib.services.logger.debug('Removing device ' + JSON.stringify(failedDevice) + ' due to permanent failure from GCM');

						c2();
					});
				});
			} else if (canonical) {
				async.forEach(canonical, function(canonicalDevice, c2) {
					var subscriptions = [];

					async.series([
						function(callback) {
							tlib.subscription.removeAllSubscriptionsFromDevice(applicationId, canonicalDevice.id, canonicalDevice.token, 'android_transport', function(err, results) {
								if (err) {
									return callback(err);
								}

								subscriptions = results;
								callback();
							});
						},
						function(callback) {
							tlib.subscription.updateDevice(applicationId, canonicalDevice.id, {
								persistent: {
									active: 1,
									token: canonicalDevice.newToken,
									type: "android"
								}
							}, callback);
						},
						function(callback) {
							if (!subscriptions.length)
								return callback();

							var tranzaction = tlib.services.redisClient.multi();

							subscriptions.forEach(function(sub) {
								tranzaction.sadd([sub, 'android_transport|'+canonicalDevice.id+'|'+canonicalDevice.newToken+'|'+applicationId]);
							});

							tranzaction.exec(callback);
						}
					], function(err) {
						if (err)
							return tlib.services.logger.error(err.message);

						tlib.services.logger.debug('Device ' + JSON.stringify(canonicalDevice) + ' changed its GCM token');

						c2();
					});
				});
			}
		}
	}
};

/**
 *
 * @param {ClientTransportMessage} message
 */
AndroidTransportWorker.prototype.processMessage = function(message) {
	var broadcast = !!message.payload['*'];
	//the default payload, used for context notifications, otherwise the real payload from message is used
	var payload = {'*': {deviceTokens: [], deltas: [], applicationId: null}};
	var self = this;
	var gcmMessage = new gcm.Message();

	async.series([
		function(callback) {
			if (broadcast) {
				payload['*'].applicationId = message.payload['*'].applicationId;
				payload['*'].deltas = message.payload['*'].deltas;

				tlib.subscription.getAllDevices(message.payload['*'].applicationId, function(err, deviceIdentifiers) {
					if (err) {
						return callback(err);
					}

					if (deviceIdentifiers.android_transport) {
						payload['*'].deviceTokens = deviceIdentifiers.android_transport;
					}

					callback();
				});
			} else {
				setImmediate(callback);
			}
		}
	], function(err) {
		if (err)
			return tlib.services.logger.error('Error: '+err.message);

		async.forEach(broadcast ? payload : message.payload, function(groupedSubscription, c) {
			var notificationsPayload = {new: [], updated: [], deleted: []};
			var devices = [];

			groupedSubscription.deviceTokens.forEach(function(device) {
				// "deviceId|gcmToken"
				var splitString = device.split('|');

				//we need this structure because of the gcm response which tells us which device token is valid or not
				devices.push({token: splitString[1], id: splitString[0]});
			});

			var applicationId = groupedSubscription.applicationId;

			async.eachSeries(groupedSubscription.deltas, function(d, c2) {
				if (d.op == 'create')
					notificationsPayload.new.push(d);
				else if (d.op == 'update')
					notificationsPayload.updated.push(d);
				else if (d.op == 'delete')
					notificationsPayload.deleted.push(d);
				c2();
			}, function() {
				if (JSON.stringify(notificationsPayload).length > 3968) {
					var collectedDeltas = {new: [], updated: [], deleted: []};

					async.forEachOfSeries(notificationsPayload, function(deltas2, key, c2) {
						async.forEach(deltas2, function(d, c3) {
							if (JSON.stringify(collectedDeltas).length + JSON.stringify(d).length < 3968)
								collectedDeltas[key].push(d);
							else {
								if (collectedDeltas.new.length + collectedDeltas.updated.length + collectedDeltas.deleted.length > 1)	{
									gcmMessage.addData({data: cloneObject(collectedDeltas)});

									self.sendNotification(applicationId, gcmMessage, groupedSubscription.deviceTokens, AndroidTransportWorker.sendCallback(devices, applicationId));
								} else {
									if (!collectedDeltas[key].length)
										collectedDeltas[key].push(d);

									self.storeAmazonLink(cloneObject(collectedDeltas), function(err, resourcePublicLink) {
										if (err) return tlib.services.logger.error(err.message);

										gcmMessage.addData({url: resourcePublicLink});
										self.sendNotification(applicationId, gcmMessage, groupedSubscription.deviceTokens, AndroidTransportWorker.sendCallback(devices, applicationId));
									});
								}

								collectedDeltas = {new: [], updated: [], deleted: []};
								gcmMessage = new gcm.Message();
								collectedDeltas[key].push(d);
							}
							c3();
						}, function() {
							if (collectedDeltas.new.length || collectedDeltas.updated.length || collectedDeltas.deleted.length) {
								gcmMessage.addData({data: cloneObject(collectedDeltas)});

								self.sendNotification(applicationId, gcmMessage, groupedSubscription.deviceTokens, AndroidTransportWorker.sendCallback(devices, applicationId));
							}
							c2();
						});
					});
				} else {
					gcmMessage.addData({data: notificationsPayload});
					self.sendNotification(applicationId, gcmMessage, groupedSubscription.deviceTokens, AndroidTransportWorker.sendCallback(devices, applicationId));
				}
				c();
			});
		});
	});
};

module.exports = AndroidTransportWorker;
