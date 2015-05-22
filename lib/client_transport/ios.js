var apn = require('apn');

var apnConnection = new apn.Connection({cert : 'certificate', key: 'privatekey'});

var Send = function(message) {
	var apnNotification = new apn.Notification();
	apnNotification.payload = message.data;

	async.each(Object.keys(message.devices), function(device, c){
		message.devices[device] = new apn.Device(pnTokens[device]);
		c();
	});

	apnConnection.pushNotification(apnNotification, message.devices);
};

module.exports = Send;
