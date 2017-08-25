var Base_Worker = require('../base_worker'),
	async = require('async'),
	fs = require('fs');

/**
 *
 * @param {int} index Index of the socket transport worker. These workers have their own queue.
 * @constructor
 */
var SocketClientTransport = function(index, config) {
	Base_Worker.call(this, 'sockets_transport', index, config);

	this.socketIo = null;
	this.sockets = {};
	this.broadcast = true;

	var self = this;

	if (!this.config.use_ssl) {
		this.socketIo = require('socket.io')(this.config.socket_port || 80);
	} else {
		var privateKeyFile = this.config.ssl_key_file;
		var certificateFile = this.config.ssl_cert_file;
		var intermediateCertificate = this.config.ssl_ca_file;
		var passphrase = this.config.ssl_key_passphrase;

		if (!privateKeyFile && !certificateFile) {
			throw new Error('SSL configuration is missing');
		}

		var httpsServer = require('https').createServer({
			key: fs.readFileSync(__dirname + '/' + privateKeyFile),
			cert: fs.readFileSync(__dirname + '/' + certificateFile),
			passphrase: passphrase || undefined,
			ca: intermediateCertificate ? [fs.readFileSync(__dirname + '/' + intermediateCertificate)] : undefined
		});
		httpsServer.listen(this.config.socket_port || 443);

		this.socketIo = require('socket.io')(httpsServer);
	}

	this.socketIo.on('connection', function(socket) {
		self.sockets[socket.id] = {socket: socket};
		tlib.services.logger.info('Socket client with id '+socket.id+' connected');

		socket.on('bind_device', function(data) {
			self.sockets[socket.id].device = {id: data.device_id, appId: data.application_id};
			async.series([
				function updateDevice(callback) {
					tlib.subscription.updateDevice(data.application_id, data.device_id, {
						volatile: {
							active: 1,
							token: socket.id,
							type: 'sockets',
							server_name: self.name
						}
					}, function(err) {
						if (err) {
							return callback(new Error('Failed updating device on bind_device: '+err.message));
						}

						callback();
					});
				},
				function restoreSubscriptions(callback) {
					tlib.subscription.getDeviceSubscriptions(data.application_id, data.device_id, function(err, subscriptions) {
						if (err) {
							return callback(err);
						}

						if (subscriptions.length) {
							var transaction = tlib.services.redisClient.multi();

							subscriptions.forEach(function(sub) {
								transaction.sadd([sub, self.name+'|'+data.device_id+'|'+socket.id+'|'+data.application_id]);
							});

							transaction.exec(function(err) {
								if (err) {
									return callback(new Error('Failed restoring subscription keys: '+err.message));
								}
								callback();
							});
						} else	{
							setImmediate(callback);
						}
					});
				}
			], function(err) {
				if (err) {
					tlib.services.logger.error(err.message);
				}
				socket.emit('ready', {});
			});
		});

		socket.on('disconnect', function() {
			tlib.services.logger.info('Socket client with id '+socket.id+' disconnected');

			if (self.sockets[socket.id].device) {
				var appId = self.sockets[socket.id].device.appId;
				var deviceId = self.sockets[socket.id].device.id;

				delete self.sockets[socket.id];
				async.series([
					function removeSubscriptions(callback) {

						tlib.subscription.removeAllSubscriptionsFromDevice(appId, deviceId, socket.id, self.name, callback);
					},
					function updateDevice(callback) {
						tlib.subscription.updateDevice(appId, deviceId, {
							volatile: {
								active: 0,
								token: null,
								server_name: null
							}
						}, function(err) {
							if (err) {
								return callback(new Error('Failed updating device on bind_device: '+err.message));
							}
							callback();
						});
					}
				], function(err) {
					if (err) {
						tlib.services.logger.error(err.message);
					}
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
 * @param {ClientTransportMessage} message
 */
SocketClientTransport.prototype.processMessage = function(message) {
	var self = this;
	var payload = message.payload;
	var broadcast = !!message.payload['*'];

	async.forEachOf(payload, function(groupedSubscription, subscriptionKey, c) {
		var messageToSend = {new: [], updated: [], deleted: []};

		if (!broadcast) {
			async.each(groupedSubscription.deltas, function(d, c2) {
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
				async.each(groupedSubscription.deviceTokens, function(deviceIdentifier, c2) {
					var token = deviceIdentifier.split('|')[1];
					var deviceId = deviceIdentifier.split('|')[0];
					if (self.sockets[token]) {
						self.sockets[token].socket.emit('message', {data: messageToSend});
					} else {
						tlib.services.logger.error('Unable to send notification to socket client "' + deviceIdentifier + '" : '
							+ 'device not connected or invalid token');
						tlib.subscription.remove(groupedSubscription.applicationId, deviceId, subscriptionKey, token, function(err) {
							if (err) {
								tlib.services.logger.error('Failed removing device "' + deviceIdentifier + '"'
									+ ' from subscription "'+ subscriptionKey +'": ' + err.message);
							}
						});
					}

					c2();
				}, c);
			});
		} else {
			async.each(payload['*'].deltas, function(delta, c) {
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
				async.each(self.sockets, function(connectedDevice, c2) {
					connectedDevice.socket.emit('message', {data: messageToSend});
					c2();
				});
			});
		}
	});
};

module.exports = SocketClientTransport;
