var gcm = require('node-gcm-service');
var pnSender = new gcm.Sender({apiKey: 'AIzaSyDGnXYHBGUWih1NamoDh2MitkplqXzBFQM'});

var send = function(message) {
	var gcmMessage = new gcm.Message();
	gcmMessage.setDataWithObject({data: message.data});

	pnSender.sendMessage(gcmMessage.toJSON(), message.devices, true, function(err, data) {
		console.log(err);
		console.log(data);
	});
};

module.exports = {
	send: send
};
