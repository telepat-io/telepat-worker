var Base_Worker = require('./base_worker');
var Models = require('telepat-models');
var async = require('async');

var AggregationWorker = function(index) {
	Base_Worker.call(this, 'aggregation', index);
};

AggregationWorker.prototype = new Base_Worker();

AggregationWorker.prototype.processMessage = function(message) {
	//type, object
	var operationtype = message.op;
	var object = null;
	var updateTimestamp = message.ts;

	var self = this;
	var subscriptionChannels = [];
	var deltaObjects = [];
	var appId = message.applicationId;

	async.waterfall([
		function getObject(callback) {
			if (operationtype == Base_Worker.OP.UPDATE) {
				var pathParts = message.object[0].path.split('/'); //[0] is modelName, [1] is objectId
				Models.Model(pathParts[0], message.applicationId, pathParts[1], function(err, result) {
					if (err) return callback(err);

					object = result;
					callback();
				});
			} else if (operationtype == Base_Worker.OP.DELETE) {
				var pathParts = message.object.path.split('/'); //[0] is modelName, [1] is objectId
				Models.Model(pathParts[0], message.applicationId, pathParts[1], function(err, result) {
					if (err) return callback(err);

					object = result;
					callback();
				});
			} else {
				object = message.object;
				callback();
			}
		},
		function ProcessOperation(callback) {
			self.processOperation(object, appId, function(err, results) {
				if (err) return callback(err);

				subscriptionChannels = results;
				callback();
			});
		},
		function formDeltas(callback) {
			if (operationtype == Base_Worker.OP.UPDATE) {
				var patches = message.object;

				self.filterLatePatches(patches, updateTimestamp, appId, function(err, filteredPatches) {
					if (err) return callback(err);

					filteredPatches.forEach(function(patch) {
						var createdDelta = Base_Worker.createDelta(patch.op, patch.value, patch.path);
						createdDelta.application_id = appId;
						deltaObjects.push(createdDelta);
					});
					callback();
				});
			} else {
				var createdDelta = null;

				if (operationtype == Base_Worker.OP.ADD)
					createdDelta = Base_Worker.createDelta(operationtype, object);
				else
					createdDelta = Base_Worker.createDelta(operationtype, undefined, message.object.path);

				createdDelta.application_id = appId;

				deltaObjects.push(createdDelta);
				callback();
			}
		},
		function writeDeltas(callback) {
			var channelsWithNewDeltas = [];

			async.each(subscriptionChannels, function(channel, c) {
				async.each(deltaObjects, function(mainDelta, c1) {
					var d = mainDelta.clone();
					d.setChannel(channel);
					Base_Worker.writeDelta(d, function(err, deltasLength) {
						if (err) return c1(err);

						if (deltasLength == 1)
							channelsWithNewDeltas.push(channel);
						c1();
					});
				}, c);
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

AggregationWorker.prototype.filterLatePatches = function(patches, updateTimestamp, appId, callback) {
	if (!patches.length)
		return callback(null, []);
	var pathParts = patches[0].path.split('/'); // modelname/id/fieldname
	var updateTimestampBaseKey = 'blg:'+appId+':'+pathParts[0]+':'+pathParts[1];

	async.filter(patches, function(itemPatch, filterCallback) {
		var modifiedProperty = itemPatch.path.split('/')[2]; //fieldname
		Models.Application.redisClient.get(updateTimestampBaseKey+':'+modifiedProperty+':mod_timestamp', function(err, result) {
			var wholeKey = updateTimestampBaseKey+':'+modifiedProperty+':mod_timestamp';
			if (result === null) {
				Models.Application.redisClient.set(wholeKey, updateTimestamp);
				Models.Application.redisClient.expire(wholeKey, 60);

				return filterCallback(true);
			}
			if (parseInt(result) < updateTimestamp) {
				Models.Application.redisClient.set(wholeKey, updateTimestamp, function(err, result) {
					Models.Application.redisClient.expire(wholeKey, 60);
				});

				return filterCallback(true);
			}

			return filterCallback(false);
		});
	}, function(results) {
		if (!results.length) {
			return callback(new Error('All patches has been discarded due to being outdated.'));
		}

		callback(null, results);
	});
};

AggregationWorker.prototype.processOperation = function(object, appId, callback) {
	var subscriptionKeys = [];

	if (object.type == Base_Worker.OBJECT_TYPE.USER || object.type == Base_Worker.OBJECT_TYPE.CONTEXT) {
		subscriptionKeys.push((new Models.Channel(appId)).model(object.type));
		subscriptionKeys.push((new Models.Channel(appId)).model(object.type, object.id));
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
	var self = this;

	if (!subscriptionKeys.length)
		return callback();

	async.map(subscriptionKeys, function(channel, c) {
		c(null, channel.get());
	}, function(err, results) {
		self.messagingClient.send([{
			topic: 'write',
			messages: [JSON.stringify({keys: results, applicationId: appId})],
			attributes: 0
		}], callback);
	});
};

module.exports = AggregationWorker;
