var gcm = require('node-gcm-service');
var pnSender = new gcm.Sender({apiKey: 'AIzaSyDGnXYHBGUWih1NamoDh2MitkplqXzBFQM'});
var async = require('async');

var send = function(message) {
	kafkaConsumer.commit(function(err, result) {
		if (!err) {
			console.log('Object.remove commited');
		} else
			console.log(err);
	});

	var gcmMessage = new gcm.Message();

	var objectTimestamps = {}; //so we don't check the same object from different subscriptions

	async.filter(message.patches.updated, function(patch, filterCallback) {
		var transportMessageTimestamp = patch._microtime;
		var pathParts = patch.path.split('/');
		delete patch._microtime;

		if (objectTimestamps[patch.path])
			return filterCallback(true);

		objectTimestamps[patch.path] = true;

		var transportMessageKey = 'blg:'+message.applicationId+':'+pathParts.join(':')+':transport_msg_timestamp';
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
		message.patches.updated = results;
		gcmMessage.setDataWithObject({data: message.patches});

		pnSender.sendMessage(gcmMessage.toJSON(), [[message.object.persistent.token]], true, function(err, data) {
			console.log(err);
			console.log(data);
		});
	});


};

module.exports = {
	send: send
};
