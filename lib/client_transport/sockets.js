var Base_Worker = require('../base_worker');
var Models = require('telepat-models');
var async = require('async');
var fs = require('fs');

var SocketClientTransport = function(index) {
	Base_Worker.call(this, 'sockets_transport', index);

	this.sockets = {};
	this.broadcast = true;

	var self = this;

	var onConnectListener = function(socket) {
		Models.Application.logger.info('Socket client with id '+socket.id+' connected');

		socket.emit('welcome', {sessionId: socket.id});
		self.sockets[socket.id] = socket;

		socket.on('bindDevice', function(data) {
			self.sockets[socket.id].device = {object: data.device, appId: data.application_id};
		});
		socket.on('disconnect', function() {
			Models.Application.logger.info('Socket client with id '+socket.id+' disconnected');

			if (self.sockets[socket.id].device) {
				var appId = self.sockets[socket.id].device.appId;
				var device = self.sockets[socket.id].device.object;

				async.series([
					function(callback) {
						Models.Subscription.removeAllSubscriptionsFromDevice(appId, device.id, function(err) {
							if (err) {
								Models.Application.logger.error('removeAllSubscriptionsFromDevice: '+err.message);
							}

							callback();
						});
					},
					function(callback) {
						if (!device.info.udid) {
							Models.Subscription.removeDevice(appId, device.id, function(err) {
								if (err) {
									Models.Application.logger.error('removeDevice: '+err.message);
								}

								callback();
							});
						} else
							callback();
					}
				], function(err) {
					delete self.sockets[socket.id];
				});
			} else {
				delete self.sockets[socket.id];
			}
		});
	};

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

		var httpsServer = require('https').createServer({key: fs.readFileSync(privateKeyFile), cert: fs.readFileSync(certificateFile)});
		httpsServer.listen(port);

		this.socketIo = require('socket.io')(httpsServer);
	}

	this.socketIo.on('connection', onConnectListener);
};

SocketClientTransport.prototype = Base_Worker.prototype;

SocketClientTransport.prototype.processMessage = function(message) {
	var device = message.device;

	if (device.volatile && device.volatile.token) {
		var clientSocket = this.sockets[device.volatile.token];
		if (clientSocket) {
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
							Models.Application.logger.notice('Delta discarded because timestamp is older than a ' +
								'previous delta on the same property and object');
							return filterCallback(false);
						}
					} else {
						filterCallback(true);
					}
				});
			}, function(results) {
				message.deltas.updated = results;

				async.some(message.deltas, function(patch, callback) {
					if (patch.length) return callback(true);

					callback(false);
				}, function(result) {
					if (result === true) {
						clientSocket.emit('message', {data: message.deltas});
					} else {
						Models.Application.logger.notice('Empty deltas. Notification will not be sent to client with ' +
							'ID: '+device.volatile.token);
					}
				});
			});
		} else {
			Models.Application.logger.debug('Device "'+device.id+'" not connected (has socket ID: "'+
				device.volatile.token+'". No message to send');
		}
	} else {
		Models.Application.logger.debug('Device "'+device.id+'" has no volatile token. No message to send.');
	}
};

module.exports = SocketClientTransport;
