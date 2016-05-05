var Base_Worker = require('./base_worker');
var Models = require('telepat-models');
var async = require('async');
var _ = require('underscore');
var crypto = require('crypto')

/**
 *
 * @param {int} index Index of the transport manager worker. These workers consume from a common message queue
 * @constructor
 */
var TransportManager = function(index) {
	Base_Worker.call(this, 'transport_manager', index);
};

TransportManager.prototype = Object.create(Base_Worker.prototype);

/**
 *
 * @param {Object} message
 * @param {Object[]} message.deltas
 */
TransportManager.prototype.processMessage = function(message) {
	var self = this;
	var deltas = message.deltas;
	var broadcast = message._broadcast;
	var collectedSubscriptions = {};
	var collectedDevices = {};
	var transportMessages = {};

	if (!broadcast) {
		async.series([
			function collectSub(callback) {
				async.each(deltas, function(d, c) {
					async.each(d.subscriptions, function(sub, c2) {
						if (!collectedSubscriptions[sub])
							collectedSubscriptions[sub] = [d];
						else
							collectedSubscriptions[sub].push(d);
						c2();
					}, c);
				}, callback);
			},
			function getDevices(callback) {
				var subscriptionIds = Object.keys(collectedSubscriptions);

				self.getDevicesFromSubscriptions(subscriptionIds, function(err, devices) {
					if (err) return callback(err);
					collectedDevices = devices;
					callback();
				});
			}, function sortDevices(callback) {
				async.each(Object.keys(collectedDevices), function(dev, c) {
					var keySplit = dev.split('|'); //0 - transport name, 1 - device id, 2 - device token, 3 - appID
					var deviceIdentifier = keySplit[1]+'|'+keySplit[2];
					if (!transportMessages[keySplit[0]])
						transportMessages[keySplit[0]] = {};
					transportMessages[keySplit[0]][deviceIdentifier] = {deltas: [], application_id: keySplit[3]};//collectedDevices[dev];

					var deltaIds = {};

					async.forEachOfSeries(collectedSubscriptions, function(deltas, sub, c2) {
						var deltaCopy = [];

						collectedSubscriptions[sub].forEach(function(item) {
							var deltaId = '';

							if (item.op == 'create' || item.op == 'delete')
								deltaId = item.object.id;
							else if (item.op == 'update') {
								var md5sum = crypto.createHash('md5');
								md5sum.update(JSON.stringify(item.patch));
								deltaId = md5sum.digest('hex');
							}

							if (!deltaIds[deltaId]) {
								var commonSubs = _.intersection(item.subscriptions, collectedDevices[dev]);

								deltaIds[deltaId] = true;
								item.subscriptions = commonSubs;

								deltaCopy.push(item);
							}
						});

						if (deltaCopy.length)
							transportMessages[keySplit[0]][deviceIdentifier].deltas =
								transportMessages[keySplit[0]][deviceIdentifier].deltas.concat(deltaCopy);

						c2();
					}, c);
				}, callback);
			}, function sendMessages(callback) {
				async.forEachOf(transportMessages, function(devices, deviceString, c) {
					var queueName = deviceString.split('|')[0];

					self.messagingClient.send([JSON.stringify({payload: devices})], queueName, function(err) {
						if (err)
							Models.Application.logger.error('Failed sending message to '+queueName+': '+err.message);
					});
					c();
				}, callback);
			}
		], function(err) {
			if (err)
				return Models.Application.logger.error(err.message);
		});
	} else {
		async.series([
			function(callback) {
				var allQueues = ['android_transport', 'ios_transport'];
				var broadcastQueues = ['sockets_transport'];

				async.forEach(allQueues, function(q, c) {
					self.messagingClient.send([JSON.stringify({payload: {'*': {deltas: deltas}}})], q, function(err) {
						if (err)
							Models.Application.logger.error('Failed sending message to '+q+': '+err.message);
					});
					c();
				});
				async.forEach(broadcastQueues, function(q, c) {
					self.messagingClient.publish([JSON.stringify({payload: {'*': {deltas: deltas}}})], q, function(err) {
						if (err)
							Models.Application.logger.error('Failed broadcasting message to '+q+': '+err.message);
					});
					c();
				});
				callback();
			}
		], function(err) {
			if (err)
				return Models.Application.logger.error(err.message);
		});
	}
};

/**
 * @callback getDevicesFromSubscriptionsCb
 * @param {Error|null} err
 * @param {Object} devices The device strings are stored as keys, the values contain an array of subscriptions from
 * that device
 */
/**
 *
 * @param {string[]} subscriptions
 * @param {getDevicesFromSubscriptionsCb} callback
 */
TransportManager.prototype.getDevicesFromSubscriptions = function(subscriptions, callback) {
	var transaction = Models.Application.redisClient.multi();

	subscriptions.forEach(function(sub) {
		transaction.smembers(sub);
	});

	transaction.exec(function(err, replies) {
		if (err) return callback(err);

		var devices = {};
		async.forEachOf(replies, function(reply, index, c) {
			async.each(reply, function(dev, c2) {
				if (!devices[dev])
					devices[dev] = [subscriptions[index]];
				else
					devices[dev].push(subscriptions[index]);
				c2();
			}, c);
		}, function() {
			callback(null, devices);
		});
	})
};

module.exports = TransportManager;
