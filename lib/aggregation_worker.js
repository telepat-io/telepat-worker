var Base_Worker = require('./base_worker');
var Models = require('telepat-models');
var async = require('async');

/**
 *
 * @param {int} index Index of the aggregation worker. Aggregation workers consume from a common message queue
 * @constructor
 */
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

				async.forEachOf(message.patches, function(p, index, c) {
					var delta = new Models.Delta(message);
					delta.patch = p;
					delta.timestamp += index;

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
		function(callback) {
			if (deltasToWrite.length) {
				Models.Application(deltasToWrite[0].object.application_id, function(err, app) {
					if (err) {
						err.message = 'Error in retrieving application with ID '+deltasToWrite[0].object.application_id+': '+err.message;
						return callback(err);
					}
					Models.Application.loadedAppModels[app.id] = app;
					callback();
				});
			} else
				callback();
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
			if (!deltasToWrite.length)
				return callback();
			self.writeDeltas(deltasToWrite, function(err, result) {
				if (err) return callback(err);

				deltasInDb = result;
				callback();
			});
		},
		function notifyWriters(callback) {
			if (deltasInDb == 0 && deltasToWrite.length)
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
 * @callback writeDeltasCb
 * @param {Error|null} err
 * @param {int} [deltasInDb] The number of deltas in the key before adding more
 */
/**
 *
 * @param {Delta[]} deltas
 * @param {writeDeltasCb} callback
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

/**
 * @callback getAffectedChannelsCb
 * @param {Error|null} err
 * @param {Channel[]} [channels]
 */
/**
 *
 * @param {object} item
 * @param {string} item.context_id
 * @param {string} item.type
 * @param {string} [item.id] Only available to operations that update or delete an object
 * @param {string} [item.user_id] Only available to items that have been created by a logged user/admin
 * @param {string} appId
 * @param {getAffectedChannelsCb} callback
 */
AggregationWorker.prototype.getAffectedChannels = function(item, appId, callback) {
	var context = item.context_id;
	var mdl = item.type;
	/**
	 *
	 * @type {Channel[]}
	 */
	var affectedChannels = [];
	var modelsBaseChannel = (new Models.Channel(appId)).model(item.type);
	affectedChannels.push(modelsBaseChannel);

	//the channel of one object
	if (item.id)
		affectedChannels.push((new Models.Channel(appId)).model(item.type, item.id));

	var builtinModels = ['application', 'admin', 'user', 'user_metadata', 'context'];

	if (builtinModels.indexOf(mdl) === -1) {
		//all objects of type  from context
		affectedChannels.push(modelsBaseChannel.clone().context(context));

		var modelSchema = Models.Application.loadedAppModels[appId].schema[mdl];
		if (modelSchema) {
			for (var r in modelSchema.belongsTo) {
				if (item[modelSchema.belongsTo[r].parentModel+'_id']) {
					//all objects with the parent
					affectedChannels.push(modelsBaseChannel.clone().parent({model: modelSchema.belongsTo[r].parentModel,
						id: item[modelSchema.belongsTo[r].parentModel+'_id']}));
					//all objects with that parent from that user
					affectedChannels.push(modelsBaseChannel.clone().parent({model: modelSchema.belongsTo[r].parentModel,
						id: item[modelSchema.belongsTo[r].parentModel+'_id']}).user(item.user_id));
				}
			}
		}

		//all objects from that user
		affectedChannels.push(modelsBaseChannel.clone().context(context).user(item.user_id));
	}

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
