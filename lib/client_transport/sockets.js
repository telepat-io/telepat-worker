var http = require('http');
var socketIo = require('socket.io')(80);

var sockets = {};

module.exports = {
	initialize: function() {
		socketIo.on('connection', function(socket) {
			sockets[socket.id] = socket;
			console.log(socket.id);
			socket.emit('welcome', {sessionId: socket.id});

			socket.on('disconnect', function() {
				delete sockets[socket.id];
			});
		});
	},
	send: function(msg) {
		var devices = msg.devices;

		for(var i in devices) {
			if (devices[i].volatile.token) {
				var clientSocket = sockets[devices[i].volatile.token];
				clientSocket.emit({data: msg.data});
			}
		}
	}
};
