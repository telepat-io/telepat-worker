var Base_Worker = require('../base_worker');
var Models = require('telepat-models');
var async = require('async');

var SocketClientTransport = function(index) {
	Base_Worker.call(this, 'sockets_transport', index);

	var port = process.env.TP_SCKT_PORT || 80;

	this.socketIo = require('socket.io')(port);
	this.sockets = {};
	this.broadcast = true;

	var self = this;

	this.socketIo.on('connection', function(socket) {
		self.sockets[socket.id] = socket;
		console.log('Socket client with id '+socket.id.blue+' connected');

		socket.emit('welcome', {sessionId: socket.id});

		socket.on('bindDevice', function(data) {
			self.sockets[socket.id].device = {object: data.device, appId: data.application_id};
		});

		socket.on('disconnect', function() {
			console.log('Socket client with id '+socket.id.blue+' disconnected');

			if (self.sockets[socket.id].device) {
				var appId = self.sockets[socket.id].device.appId;
				var device = self.sockets[socket.id].device.object;

				async.series([
					function(callback) {
						Models.Subscription.removeAllSubscriptionsFromDevice(appId, device.id, callback);
					},
					function(callback) {
						if (!device.info.udid) {
							Models.Subscription.removeDevice(appId, device.id, callback);
						} else
							callback();
					}
				], function(err) {
					if (err) console.log(err);
					delete self.sockets[socket.id];
				});
			}
		});
	});
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
							console.log('Delta discarded'.red);
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
						console.log('Sent notification to client with ID: '+device.volatile.token.blue);
					} else {
						console.log('Warning'.red+': empty deltas. Notification will not be sent to client with ID: '+device.volatile.token.blue);
					}
				});
			});
		}
	}
};

module.exports = SocketClientTransport;
