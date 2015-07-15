var apn = require('apn');
var fs = require('fs');

var apnConnection = new apn.Connection({key: './lib/client_transport/key.pem', cert: './lib/client_transport/cert', passphrase: 'ficat+telepat'});

var Send = function(message) {
	var apnNotification = new apn.Notification();
	if (message.object.persistent && message.object.persistent.token) {
		var iosDevice = new apn.Device(message.object.persistent.token);
		apnNotification.payload = {data: message.patches};
		apnNotification['content-available'] = 1;

		apnConnection.pushNotification(apnNotification, iosDevice);
	} else {
		console.log('Device '+message.object.id+' is missing push notification token');
	}
};

module.exports = {
	send: Send
};
