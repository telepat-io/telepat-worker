var socketIo = require('socket.io')(80);
var async = require('async');

var sockets = {};

module.exports = {
	initialize: function() {
		socketIo.on('connection', function(socket) {
			sockets[socket.id] = socket;
			console.log('Socket client with id '+socket.id.blue+' connected');
			socket.emit('welcome', {sessionId: socket.id});

			socket.on('disconnect', function() {
				console.log('Socket client with id '+socket.id.blue+' disconnected');
				delete sockets[socket.id];
			});
		});
	},
	send: function(msg) {
		var device = msg.object;

		kafkaConsumer.commit(function(err, result) {
			if (!err) {
				console.log('Object.remove commited');
			} else
				console.log(err);
		});

		if (device.volatile.token) {
			var clientSocket = sockets[device.volatile.token];
			if (clientSocket) {
				var objectTimestamps = {}; //so we don't check the same object from different subscriptions

				async.filter(msg.patches.updated, function(patch, filterCallback) {
					var transportMessageTimestamp = patch._microtime;
					var pathParts = patch.path.split('/');
					delete patch._microtime;

					if (objectTimestamps[patch.guid])
						return filterCallback(true);

					objectTimestamps[patch.guid] = true;

					var transportMessageKey = 'blg:'+msg.applicationId+':'+pathParts.join(':')+':transport_msg_timestamp';
					redisClient.get(transportMessageKey, function(err, result) {
						if (result) {
							if (parseInt(result) <= transportMessageTimestamp)
								return filterCallback(true);
							else
								return filterCallback(false);
						} else {
							filterCallback(true);
						}
					});
				}, function(results) {
					msg.patches.updated = results;

					async.some(msg.patches, function(patch, callback) {
						if (patch.length) return callback(true);

						callback(false);
					}, function(result) {
						if (result === true) {
							clientSocket.emit('message', {data: msg.patches});
						}
					});
				});
			}
		}
	}
};
