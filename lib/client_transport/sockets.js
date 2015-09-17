var Base_Worker = require('../base_worker');
var Models = require('telepat-models');
var async = require('async');

var SocketClientTransport = function(index) {
	Base_Worker.call(this, 'sockets_client', index);

	var port = process.env.TP_SCKT_PORT || 80;

	this.socketIo = require('socket.io')(port);
	this.sockets = {};

	this.socketIo.on('connection', (function(socket) {
		this.sockets[socket.id] = socket;
		console.log('Socket client with id '+socket.id.blue+' connected');
		socket.emit('welcome', {sessionId: socket.id});

		socket.on('disconnect', (function() {
			console.log('Socket client with id '+socket.id.blue+' disconnected');
			delete this.sockets[socket.id];
		}).bind(this));
	}).bind(this));
};

SocketClientTransport.prototype = Base_Worker.prototype;

SocketClientTransport.prototype.shutdown = function() {
	Base_Worker.prototype.shutdown.call(this, function() {
		console.log('Closing web socket server...');
		this.socketIo.close();
	});
};

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

				var transportMessageKey = 'blg:'+message.applicationId+':'+pathParts.join(':')+':transport_msg_timestamp';
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
