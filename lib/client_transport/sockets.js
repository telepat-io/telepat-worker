var Base_Worker = require('../base_worker'),
	Models = require('telepat-models'),
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
	var port = null;

	if (!process.env.TP_SCKT_SSL) {
		port = process.env.TP_SCKT_PORT || 80;
		this.socketIo = require('socket.io')(port);
	} else {
		port = process.env.TP_SCKT_PORT || 443;
		var privateKeyFile = process.env.TP_SCKT_KEY_SSL || this.config.ssl_key_file;
		var certificateFile = process.env.TP_SCKT_CERT_SSL || this.config.ssl_cert_file;

		if (!privateKeyFile && !certificateFile) {
			throw new Error('SSL configuration is missing');
		}

		var httpsServer = require('https').createServer({key: fs.readFileSync(__dirname+'/'+privateKeyFile), cert: fs.readFileSync(__dirname+'/'+certificateFile)});
		httpsServer.listen(port);

		this.socketIo = require('socket.io')(httpsServer);
	}

	this.socketIo.on('connection', function(socket) {
		self.sockets[socket.id] = {socket: socket};
		Models.Application.logger.info('Socket client with id '+socket.id+' connected');

		socket.on('bind_device', function(data) {
			self.sockets[socket.id].device = {id: data.device_id, appId: data.application_id};
		//	console.log("binded device and socket is ", socket.id);
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
						if (err) {
							return callback(new Error('Failed updating device on bind_device: '+err.message));
						}
				
						callback();
					});
				},
				function restoreSubscriptions(callback) {
					Models.Subscription.getDeviceSubscriptions(data.application_id, data.device_id, function(err, subscriptions) {
						if (err) {
							return callback(err);
						}
					//	console.log("subsriptions length", subscriptions.length);
						if (subscriptions.length) {
							var transaction = Models.Application.redisClient.multi();
							
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
				//console.log ("this err");
				if (err) {
					Models.Application.logger.error(err.message);
				}
				socket.emit('ready', {});
			});
		});

		socket.on('disconnect', function() {
			Models.Application.logger.info('Socket client with id '+socket.id+' disconnected');

			if (self.sockets[socket.id].device) {
				var appId = self.sockets[socket.id].device.appId;
				var deviceId = self.sockets[socket.id].device.id;

				delete self.sockets[socket.id];
				async.series([
					function removeSubscriptions(callback) {
						//console.log("removed subscription");
						Models.Subscription.removeAllSubscriptionsFromDevice(appId, deviceId, socket.id, self.name, callback);
					},
					function updateDevice(callback) {
						Models.Subscription.updateDevice(appId, deviceId, {
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
						Models.Application.logger.error(err.message);
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
		//	console.log("first thing");
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
				//	console.log("message" + messageToSend.toString());
					console.log("token is ", token);
					if (self.sockets[deviceId]) {
						self.sockets[deviceId].socket.emit('message', {data: messageToSend});
					} else {
						Models.Application.logger.error('Unable to send notification to socket client "' + deviceIdentifier + '" : '
							+ 'device not connected or invalid token');
						Models.Subscription.remove(groupedSubscription.applicationId, deviceId, subscriptionKey, token, function(err) {
							console.log("appId = ", groupedSubscription.applicationId, "device id = ", deviceId, "subscription key = ", subscriptionKey, "token = ", token);
							if (err) {
								Models.Application.logger.error('Failed removing device "' + deviceIdentifier + '"'
									+ ' from subscription "'+ subscriptionKey +'": ' + err.message);
							}
						});
					}

					c2();
				}, c);
			});
		} else {
			async.each(payload['*'].deltas, function(delta, c) {
			//	console.log(delta.op);
			///	console.log(messageToSend.toString());
			 //  console.log("second thing");
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
				//	console.log("socket should emit message" + messageToSend);
					connectedDevice.socket.emit('message', {data: messageToSend});
					c2();
				});
			});
		}
	});
};

module.exports = SocketClientTransport;
