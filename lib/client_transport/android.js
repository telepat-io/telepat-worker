var gcm = require('node-gcm-service');
var pnSender = new gcm.Sender({key: 'apiKey'});

var Send = function(message) {
	var gcmMessage = {data: message.data};

	pnSender.sendMessage(gcmMessage.toJSON(), message.devices, true, function(err, data) {

	});
};

module.exports = Send;
