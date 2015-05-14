var apn = require('apn');

var apnConnection = new apn.Connection({cert : 'certificate', key: 'privatekey'});

var Send = function(pnTokens, data) {
	var apnNotification = new apn.Notification();
	apnNotification.payload = data;

	async.each(Object.keys(pnTokens), function(device, c){
		pnTokens[device] = new apn.Device(pnTokens[device]);
		c();
	});

	apnConnection.pushNotification(apnNotification, pnTokens);
};

module.exports = Send;
