var Base_Worker = require('./base_worker');
var Models = require('telepat-models');
var async = require('async');

var AggregationWorker = function(index) {
	Base_Worker.call(this, 'aggregation', index);
};

AggregationWorker.prototype = Object.create(Base_Worker.prototype);

/**
 *
 * @param {Object} message
 * @param {string} message.op
 * @param {string} message.application_id
 * @param {Object[]} message.patches
 * @param {Object} message.object
 * @param {string} message.object.id
 * @param {string} message.object.model
 * @param {Number} message.timestamp Only for updates
 */
AggregationWorker.prototype.processMessage = function(message) {
	var self = this;
	var deltasToWrite = [];
	var deltasInDb = 0;

	async.series([
		function createDelta(callback) {
			var delta = new Models.Delta(message);

			if (message.op == 'create')	{
				deltasToWrite.push(delta);
				callback();
			} else if (message.op == 'update') {
				var collectedObjects = {};

				async.each(message.patches, function(p, c) {
					var delta = new Models.Delta(message);
					delta.patch = p;

					if (collectedObjects[p.path.split('/')[1]]) {
						delta.object = collectedObjects[p.path.split('/')[1]];
						c();
					} else {
						Models.Model(p.path.split('/')[1], function(err, result) {
							if (err)
								Models.Application.logger.warning(err.message);
							else {
								delta.object = result;
								deltasToWrite.push(delta);
							}
							c();
						});
					}
				}, callback);
			} else if (message.op == 'delete') {
				Models.Model(message.object.id, function(err, result) {
					if (err)
						callback(err);
					else {
						delta.object = result;
						deltasToWrite.push(delta);
						callback();
					}
				});
			}
		},
		function getAffectedChannels(callback) {
			async.each(deltasToWrite, function(d, c) {
				self.getAffectedChannels(d.object, d.application_id, function(err, channels) {
					if (err)
						return c(new Error('Failed to get affected channels: '+err.message));
					d.subscriptions = channels.map(function(channel) {
						return channel.get();
					});
					if (d.op == 'update')
						delete d.object;
					c();
				});
			}, callback);
		},
		function writeDeltas(callback) {
			self.writeDeltas(deltasToWrite, function(err, result) {
				if (err) return callback(err);

				deltasInDb = result;
				callback();
			});
		},
		function notifyWriters(callback) {
			if (deltasInDb == 0)
				self.messagingClient.send([JSON.stringify({key: [self.name+':deltas']})], 'write', callback);
			else
				callback();
		}
	], function(err) {
		if (err)
			Models.Application.logger.error(err.message+"\n"+err.stack);
	});
};

/**
 *
 * @param {Delta[]} deltas
 * @param callback
 */
AggregationWorker.prototype.writeDeltas = function(deltas, callback) {
	var transaction = Models.Application.redisClient.multi();
	var zaddArgs = [this.name+':deltas'];
	var self = this;

	async.each(deltas, function(d, c) {
		zaddArgs.push(d.timestamp, JSON.stringify(d.toObject()));
		c();
	}, function() {
		transaction.zcard(self.name+':deltas');
		transaction.zadd(zaddArgs);
		transaction.exec(function(err, replies) {
			if (err)
				return callback(new Error('Transaction failed: writing delta object to key "'+
					self.name+':deltas" ('+err.toString()+')'));

			//returns number of deltas in the key
			callback(null, replies[0]);
		});
	});
};

AggregationWorker.prototype.getAffectedChannels = function(item, appId, callback) {
	var context = item.context_id;
	var mdl = item.type;
	var parent = {};
	var affectedChannels = [];
	var modelsBaseChannel = (new Models.Channel(appId)).model(item.type);
	affectedChannels.push(modelsBaseChannel);

	//the channel of one object
	affectedChannels.push((new Models.Channel(appId)).model(item.type, item.id));
	//all objects of type  from context
	affectedChannels.push(modelsBaseChannel.clone().context(context));

	var modelSchema = Models.Application.loadedAppModels[appId].schema[mdl];
	if (modelSchema) {
		for (var r in modelSchema.belongsTo) {
			if (item[modelSchema.belongsTo[r].parentModel+'_id']) {
				parent = {model: modelSchema.belongsTo[r].parentModel,
					id: item[modelSchema.belongsTo[r].parentModel+'_id']};
			}
		}
	}

	//all objects with the parent
	affectedChannels.push(modelsBaseChannel.clone().parent(parent));
	//all objects from that user
	affectedChannels.push(modelsBaseChannel.clone().context(context).user(item.user_id));
	//all objects with that parent from that user
	affectedChannels.push(modelsBaseChannel.clone().parent(parent).user(item.user_id));

	async.filter(affectedChannels, function(channelItem, c) {
		c(channelItem.isValid());
	}, function(validChannels) {
		var channelsWithFilters = [];
		async.each(validChannels, function(channel, eachCallback) {
			Models.Subscription.getSubscriptionKeysWithFilters(channel, function(err, filteredChannels) {
				if (err) {
					err.message = 'getSubscriptionKeysWithFilters error: '+err.message;

					return eachCallback(err);
				}

				async.each(filteredChannels, function(filteredChannel, c) {
					if (Models.utils.testObject(item, filteredChannel.filter)) {
						channelsWithFilters.push(filteredChannel);
					}
					c();
				}, eachCallback);
			});
		}, function(err) {
			if (err) return callback(err);

			callback(null, validChannels.concat(channelsWithFilters));
		});
	});
};

module.exports = AggregationWorker;
