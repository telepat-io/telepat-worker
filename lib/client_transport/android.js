var gcm = require('node-gcm-service');
var pnSender = new gcm.Sender({key: 'apiKey'});

var Send = function(pnTokens, data) {
	var gcmMessage = {data: data};

	pnSender.sendMessage(gcmMessage.toJSON(), pnTokens, true, function(err, data) {

	});
};

module.exports = Send;
