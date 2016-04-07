var Base_Worker = require('../base_worker');
var Models = require('telepat-models');
var async = require('async');

/**
 *
 * @param {int} index Index of the socket transport worker. These workers have their own queue.
 * @constructor
 */
var SocketClientTransport = function(index) {
	Base_Worker.call(this, 'sockets_transport', index);

	var port = process.env.TP_SCKT_PORT || 80;

	this.socketIo = require('socket.io')(port);
	this.sockets = {};
	this.broadcast = true;

	var self = this;

	this.socketIo.on('connection', function(socket) {
		self.sockets[socket.id] = {socket: socket};
		Models.Application.logger.info('Socket client with id '+socket.id+' connected');

		socket.on('bind_device', function(data) {
			self.sockets[socket.id].device = {id: data.device_id, appId: data.application_id};

			async.series([
				function updateDevice(callback) {
					Models.Subscription.updateDevice(data.application_id, data.device_id, {
						volatile: {
							active: 1,
							token: socket.id,
							type: 'sockets',
							server_name: self.name
						}
					}, function(err) {
						if (err) return callback(new Error('Failed updating device on bind_device: '+err.message));
						callback();
					});
				},
				function restoreSubscriptions(callback) {
					Models.Subscription.getDevice(data.application_id, data.device_id, function(err, device) {
						if (err) return callback(err);

						if (device.subscriptions.length) {
							var transaction = Models.Application.redisClient.multi();

							device.subscriptions.forEach(function(sub) {
								transaction.sadd([sub, self.name+'|'+socket.id+'|'+data.application_id]);
							});

							transaction.exec(function(err) {
								if (err) return callback(new Error('Failed restoring subscription keys: '+err.message));
								callback();
							});
						} else
							callback();

					});
				}
			], function(err) {
				if (err) Models.Application.logger.error(err.message);
				socket.emit('ready', {});
			});
		});

		socket.on('disconnect', function() {
			Models.Application.logger.info('Socket client with id '+socket.id+' disconnected');

			if (self.sockets[socket.id].device) {
				var appId = self.sockets[socket.id].device.appId;
				var deviceId = self.sockets[socket.id].device.id;

				async.series([
					function updateDevice(callback) {
						Models.Subscription.updateDevice(appId, deviceId, {
							volatile: {
								active: 0
							}
						}, function(err) {
							if (err) return callback(new Error('Failed updating device on bind_device: '+err.message));
							callback();
						});
					},
					function removeSubscriptions(callback) {
						Models.Subscription.getDevice(appId, deviceId, function(err, device) {
							if (err) return callback(err);

							if (device.subscriptions.length) {
								var transaction = Models.Application.redisClient.multi();

								device.subscriptions.forEach(function(sub) {
									transaction.srem([sub, self.name+'|'+socket.id+'|'+appId]);
								});

								transaction.exec(function(err) {
									if (err) return callback(new Error('Failed removing subscription keys: '+err.message));
									callback();
								});
							} else
								callback();

						});
					}
				], function(err) {
					if (err) Models.Application.logger.error(err.message);

					delete self.sockets[socket.id];
				});

			} else {
				delete self.sockets[socket.id];
			}
		});
	});
};

SocketClientTransport.prototype = Object.create(Base_Worker.prototype);

/**
 *
 * @param {Object} message
 * @param {object} message.payload An object that has the GCM token as the key and the deltas its value
 */
SocketClientTransport.prototype.processMessage = function(message) {
	var self = this;
	var payload = message.payload;

	async.forEachOf(payload, function(deltas, deviceToken, c) {
		var messageToSend = {new: [], updated: [], deleted: []};

		if (deviceToken != '*') {
			if (!self.sockets[deviceToken])
				return c();

			async.forEach(deltas.deltas, function(d, c2) {
				switch(d.op) {
					case 'create': {
						messageToSend.new.push(d);

						break;
					}
					case 'update': {
						messageToSend.updated.push(d);

						break;
					}
					case 'delete': {
						messageToSend.deleted.push(d);

						break;
					}
				}
				c2();
			}, function() {
				self.sockets[deviceToken].socket.emit('message', {data: messageToSend});
				c();
			});
		} else {
			async.forEach(payload['*'].deltas, function(delta, c) {
				switch(delta.op) {
					case 'create': {
						messageToSend.new.push(delta);

						break;
					}
					case 'update': {
						messageToSend.updated.push(delta);

						break;
					}
					case 'delete': {
						messageToSend.deleted.push(delta);

						break;
					}
				}
				c();
			}, function() {
				async.forEach(self.sockets, function(connectedDevice, c) {
					connectedDevice.socket.emit('message', {data: messageToSend});
					c();
				});
			});
		}
	});
};

module.exports = SocketClientTransport;
