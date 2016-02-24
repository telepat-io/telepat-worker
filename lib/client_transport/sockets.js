var Base_Worker = require('../base_worker');
var Models = require('telepat-models');
var async = require('async');

var SocketClientTransport = function(index) {
	Base_Worker.call(this, 'sockets_transport', index);

	var port = process.env.TP_SCKT_PORT || 80;

	this.socketIo = require('socket.io')(port);
	this.sockets = {};
	this.tokens = {};
	this.broadcast = true;
	this.messageCount = {};

	var self = this;

	this.socketIo.on('connection', function(socket) {
		self.sockets[socket.id] = socket;
		Models.Application.logger.info('Socket client with id '+socket.id+' connected');

		socket.emit('welcome', {session_id: socket.id, server_name: self.name});

		socket.on('bindDevice', function(data) {
			self.sockets[socket.id].device = {id: data.device_id, appId: data.application_id};
			self.tokens[data.device_id] = socket.id;
		});

		socket.on('disconnect', function() {
			Models.Application.logger.info('Socket client with id '+socket.id+' disconnected');

			if (self.sockets[socket.id])
				delete self.tokens[self.sockets[socket.id].device.id];
			delete self.sockets[socket.id];

		});
	});
};

SocketClientTransport.prototype = Base_Worker.prototype;

SocketClientTransport.prototype.processMessage = function(message) {
	var self = this;

	var subscriptions = message.subscriptions;
	var deltas = message.deltas;
	var emptyDeltas = false;

	var allDevices = {};
	var subscriptionDevices = {};

	async.series([
		function(callback) {
			var objectTimestamps = {}; //so we don't check the same object from different subscriptions

			async.filter(message.deltas.updated, function(patch, filterCallback) {
				var transportMessageTimestamp = patch._microtime;
				var pathParts = patch.path.split('/');
				delete patch._microtime;

				if (objectTimestamps[patch.guid])
					return filterCallback(true);

				objectTimestamps[patch.guid] = true;

				var transportMessageKey = 'blg:'+patch.applicationId+':'+pathParts.join(':')+':transport_msg_timestamp';
				Models.Application.redisClient.get(transportMessageKey, function(err, result) {
					if (result) {
						if (parseInt(result) <= transportMessageTimestamp){
							return filterCallback(true);
						} else {
							Models.Application.logger.debug('Delta discarded because timestamp is older than a ' +
								'previous delta on the same property and object');
							return filterCallback(false);
						}
					} else {
						filterCallback(true);
					}
				});
			}, function(results) {
				deltas.updated = results;
				callback();
			});
		},
		function(callback) {
			async.some(message.deltas, function(patch, callback1) {
				if (patch.length) return callback1(true);

				callback1(false);
			}, function(result) {
				if (result === false) {
					Models.Application.logger.debug('Empty deltas. Notification will not be sent');
					emptyDeltas = true;
				}

				callback();
			});
		},
		function(callback) {
			if (emptyDeltas)
				return callback();
			if (subscriptions.length) {
				async.series([
					function(callback1) {
						async.each(subscriptions, function(subscription, c) {
							subscriptionDevices[subscription] = {};

							Base_Worker.getDeviceIdsFromSubscription(subscription, function(err, results) {
								if (err) {
									err.messsage = 'getDeviceIdsFromSubscription error for subscription '+subscription+': '
										+err.message;
									return c(err);
								}

								async.each(results, function(item, c2) {
									var device = item.split('|')[0];

									if (!self.tokens[device])
										return c2();

									if (!allDevices[device])
										allDevices[device] = true;

									subscriptionDevices[subscription][device] = true;
									c2();
								}, c);
							});
						}, callback1);
					},
					function(callback1) {
						async.each(Object.keys(allDevices), function(dev, c) {
							var deviceDelta = {new: [], updated: [], deleted: []};

							async.parallel([
								function(parallelCallback) {
									async.each(deltas.new, function(delta, c2) {
										if (subscriptionDevices[delta.subscription][dev])
											deviceDelta.new.push(delta);
										c2();
									}, parallelCallback);
								},
								function(parallelCallback) {
									async.each(deltas.updated, function(delta, c2) {
										if (subscriptionDevices[delta.subscription][dev])
											deviceDelta.updated.push(delta);
										c2();
									}, parallelCallback);
								},
								function(parallelCallback) {
									async.each(deltas.deleted, function(delta, c2) {
										if (subscriptionDevices[delta.subscription][dev])
											deviceDelta.deleted.push(delta);
										c2();
									}, parallelCallback);
								}
							], function() {
								async.some(deviceDelta, function(patch, callback2) {
									if (patch.length) return callback2(true);

									callback2(false);
								}, function(result) {
									if (result === false) {
										Models.Application.logger.debug('Empty deltas. Notification will not be sent');
									} else {
										self.sockets[self.tokens[dev]].emit('message', {data: deviceDelta});
										var currentMinute = (new Date()).getMinutes();

										if (Object.keys(self.messageCount).length == 0) {
											self.messageCount[currentMinute] = 1;
										} else if (self.messageCount[currentMinute]) {
											self.messageCount[currentMinute]++;
										} else {
											var lastMinute = currentMinute == 0 ? 59 : currentMinute - 1;
											console.log('==================== Minute '+currentMinute+':\t'+self.messageCount[lastMinute] || 0);
											self.messageCount[currentMinute] = 1;
										}
									}
								});

								c();
							});
						}, callback1);
					}
				], callback);
			} else {
				async.each(self.tokens, function(socketId, c) {
					self.sockets[socketId].emit('message', deltas);
					c();
				}, callback);
			}
		}
	], function(err) {
		if (err) Models.Application.logger.error('Error processing message: '+err);
	});
};

module.exports = SocketClientTransport;
