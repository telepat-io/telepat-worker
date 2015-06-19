var socketIo = require('socket.io')(80);

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
		var devices = msg.devices;

		for(var i in devices) {
			if (devices[i].volatile.token) {
				var clientSocket = sockets[devices[i].volatile.token];
				if (clientSocket)
					clientSocket.emit('message', {data: msg.data});
			}
		}
	}
};
