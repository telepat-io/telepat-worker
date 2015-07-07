var Base_Worker = require('./base_worker');
var Models = require('telepat-models');
var async = require('async');

var AggregationWorker = function(index) {
	Base_Worker.call(this, 'aggregation', index);
};

AggregationWorker.prototype = new Base_Worker();

AggregationWorker.OBJECT_TYPE = {
	USER: 0,
	CONTEXT: 1,
	MODEL: 2
};

AggregationWorker.prototype.processMessage = function(message) {
	//type, object
	var operationtype = message.op;
	var object = null;
	var objectType = null;

	var self = this;

	async.waterfall([
		function getObject(callback) {
			if (operationtype == Base_Worker.OP.UPDATE || operationtype == Base_Worker.OP.DELETE) {
				var pathParts = message.path.split('/'); //[0] is modelName, [1] is objectId
				Models.Model(pathParts[0], message.applicationId, pathParts[1], function(err, result) {
					if (err) return callback(err);

					object = result;
					objectType = result.type;
					callback();
				});
			} else {
				object = message.object;
				objectType = message.object.type;
				callback();
			}
		},
		function ProcessOperation(callback) {
			self.processOperation(objectType, object, callback);
		},
		function writeDeltas(subscriptionChannels, callback) {
			var mainDelta = Base_Worker.createDelta(operationtype, message.value, message.path);
			var channelsWithNewDeltas = [];

			async.each(subscriptionChannels, function(channel, c) {
				var d = mainDelta.clone();
				d.setChannel(channel);
				Base_Worker.writeDelta(d, function(err, deltasLength) {
					if (err) return c(err);

					if (deltasLength == 1)
						channelsWithNewDeltas.push(channel);
				});
			}, function(err) {
				if (err) return callback(err);

				callback(null, channelsWithNewDeltas);
			});
		},
		function notifyWorkers(channelsWithNewDeltas, callback) {
			if (channelsWithNewDeltas.length === 0)
				return callback();

			self.notifyWriter(channelsWithNewDeltas, message.applicationId, callback);
		}
	], function(err) {
		if (err) console.log(err);
	});
};

AggregationWorker.prototype.processOperation = function(objectType, object, callback) {
	var subscriptionKeys = [];

	if (objectType == AggregationWorker.OBJECT_TYPE.USER || objectType == AggregationWorker.OBJECT_TYPE.CONTEXT) {
		subscriptionKeys = [(new Models.Channel(object.application_id)).model(objectType)];
		callback(null, subscriptionKeys);
	}
	else {
		this.getAffectedChannels(object, function(err, channels) {
			if (err)
				return callback(err);

			channels.forEach(function(channel) {
				subscriptionKeys.push(channel);
			});

			callback(null, subscriptionKeys);
		});
	}
};

AggregationWorker.prototype.notifyWriter = function(subscriptionKeys, appId, callback) {
	if (!subscriptionKeys.length)
		return callback();
	console.log(subscriptionKeys);
	return callback();
	/*async.map(subscriptionKeys, function(channel, c) {
		c(null, channel.get());
	}, function(err, results) {
		this.messagingClient.send([{
			topic: 'write',
			messages: [JSON.stringify({keys: results, applicationId: appId})],
			attributes: 0
		}], callback);
	});*/
};

module.exports = AggregationWorker;
