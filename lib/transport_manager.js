var Base_Worker = require('./base_worker');
var Models = require('telepat-models');
var async = require('async');

var TransportManager = function(index) {
	Base_Worker.call(this, 'transport_manager', index);
};

TransportManager.prototype = Object.create(Base_Worker.prototype);

TransportManager.prototype.processMessage = function(message) {
	var self = this;
	var deltas = message.deltas;
	var collectedSubscriptions = {};
	var collectedDevices = {};
	var transportMessages = {};

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

			self.getDevicesFromSubscription(subscriptionIds, function(err, devices) {
				if (err) return callback(err);
				collectedDevices = devices;
				callback();
			});
		}, function sortDevices(callback) {
			async.each(Object.keys(collectedDevices), function(dev, c) {
				var keySplit = dev.split('|'); //0 - transport name, 1 - device token, 2 - appid
				transportMessages[keySplit[0]] = {};
				transportMessages[keySplit[0]][keySplit[1]] = {deltas: []};//collectedDevices[dev];

				async.each(collectedDevices[dev], function(sub, c2) {
					if (collectedSubscriptions[sub]) {
						transportMessages[keySplit[0]][keySplit[1]].deltas =
							transportMessages[keySplit[0]][keySplit[1]].deltas.concat(collectedSubscriptions[sub]);
					}

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
	})
};

TransportManager.prototype.getDevicesFromSubscription = function(subscriptions, callback) {
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
