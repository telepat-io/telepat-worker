var Base_Worker = require('./base_worker');
var async = require('async');
var tlib = require('telepat-models');

/**
 *
 * @param {int} index Index of the aggregation worker. Aggregation workers consume from a common message queue
 * @constructor
 */
var AggregationWorker = function (index, config) {
	Base_Worker.call(this, 'aggregation', index, config);
};

AggregationWorker.prototype = Object.create(Base_Worker.prototype);

/**
 *
 * @param {AggregatorMessage} message
 */
AggregationWorker.prototype.processMessage = function (message) {
	var self = this;
	var deltasToWrite = [];
	var deltasInDb = 0;

	async.series([
		function createDelta(callback) {
			var delta = new tlib.delta(message);

			if (message.op == 'create') {
				deltasToWrite.push(delta);
				callback();
			} else if (message.op == 'update') {
				var collectedObjects = {};

				async.forEachOf(message.patches, function (p, index, c) {
					var delta = new tlib.delta(message);
					delta.patch = p;
					delta.timestamp += index;

					if (collectedObjects[p.path.split('/')[1]]) {
						self.validateAuthor(collectedObjects[p.path.split('/')[1]], 'write_acl', delta.patch.user_id, function (err) {
							if (!err) {
								delta.object = collectedObjects[p.path.split('/')[1]];
								delete delta.patch.user_id;
							} else {
								tlib.services.logger.error(err.message);
							}
							c();
						});
					} else {
						if (p.path.split('/')[0] == 'user') {
							tlib.users.get({ "id": p.path.split('/')[1] }, delta.application_id, function (err, result) {
								if (err) {
									tlib.services.logger.error(err.message);
									c();
								} else {
									self.validateAuthor(result, 'write_acl', delta.patch.user_id, function (err1) {
										if (!err1) {
											delta.object = result.properties;
											delete delta.patch.user_id;
											deltasToWrite.push(delta);
										} else {
											tlib.services.logger.error(err1.message);
										}
										c();
									});
								}

							});
						} else {
							tlib.models.get(p.path.split('/')[1], function (err, result) {
								if (err) {
									tlib.services.logger.error(err.message);
									c();
								} else {
									self.validateAuthor(result, 'write_acl', delta.patch.user_id, function (err1) {
										if (!err1) {
											delta.object = result;
											delete delta.patch.user_id;
											deltasToWrite.push(delta);
										} else {
											tlib.services.logger.error(err1.message);
										}
										c();
									});
								}
							});
						}
					}
				}, callback);
			} else if (message.op == 'delete') {
				if (message.object.model == 'user') {
					tlib.users.delete({ "id": message.object.id, application_id: message.application_id}, function (err, result) {
						if (err) {
							tlib.services.logger.error(err.message);
							callback();
						} else {
							 callback();
						}
					});
				} else {
					tlib.models.delete(message.object.id, function (err, result) {
						if (err)
							callback(err);
						else {
							if (result) {
								self.validateAuthor(result, 'write_acl', message.user_id, function (err1) {

									if (!err1) {
										delta.object = result;
										deltasToWrite.push(delta);
									} else {
										tlib.services.logger.error(err1.message);
									}
									callback();
								});
							}
						}
					});
				}
			}
		},
		function getAffectedChannels(callback) {
			async.each(deltasToWrite, function (d, c) {
				self.getAffectedChannels(d.object, d.application_id || message.application_id, function (err, channels) {
					if (err)
						return c(new Error('Failed to get affected channels: ' + err.message));
					d.subscriptions = channels.map(function (channel) {
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
			self.writeDeltas(deltasToWrite, function (err, result) {
				if (err) return callback(err);
				deltasInDb = result;
				callback();
			});
		},
		function notifyWriters(callback) {

			if (deltasInDb == 0 && deltasToWrite.length) {
				self.messagingClient.send([JSON.stringify({ key: [self.name + ':deltas'] })], 'write', callback);
			}
			else
				callback();
		}
	], function (err) {

		if (err)
			tlib.services.logger.error(err.message + "\n" + err.stack);
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
AggregationWorker.prototype.writeDeltas = function (deltas, callback) {
	var transaction = tlib.services.redisClient.multi();
	var zaddArgs = [this.name + ':deltas'];
	var self = this;
	async.each(deltas, function (d, c) {
		zaddArgs.push(d.timestamp, JSON.stringify(d.toObject()));
		c();
	}, function () {
		transaction.zcard(self.name + ':deltas');
		transaction.zadd(zaddArgs);
		transaction.exec(function (err, replies) {
			if (err)
				return callback(new Error('Transaction failed: writing delta object to key "' +
					self.name + ':deltas" (' + err.toString() + ')'));

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
AggregationWorker.prototype.getAffectedChannels = function (item, appId, callback) {
	var context = item.context_id;
	var md1 = item.type;
	/**
	 *
	 * @type {Channel[]}
	 */
	var affectedChannels = [];

	var modelsBaseChannel = (new tlib.channel(appId)).model(item.type);
	affectedChannels.push(modelsBaseChannel);

	//the channel of one object
	if (item.id)
		affectedChannels.push((new tlib.channel(appId)).model(item.type, item.id));


	if (tlib.apps.isBuiltInModel(md1) === -1) {
		//all objects of type  from context
		affectedChannels.push(modelsBaseChannel.clone().context(context));

		var modelSchema = tlib.apps[appId].modelSchema(md1);
		if (modelSchema) {
			for (var r in modelSchema.belongsTo) {
				if (item[modelSchema.belongsTo[r].parentModel + '_id']) {
					//all objects with the parent
					affectedChannels.push(modelsBaseChannel.clone().parent({
						model: modelSchema.belongsTo[r].parentModel,
						id: item[modelSchema.belongsTo[r].parentModel + '_id']
					}));
					//all objects with that parent from that user
					affectedChannels.push(modelsBaseChannel.clone().parent({
						model: modelSchema.belongsTo[r].parentModel,
						id: item[modelSchema.belongsTo[r].parentModel + '_id']
					}).user(item.user_id));
				}
			}
		}

		//all objects from that user
		affectedChannels.push(modelsBaseChannel.clone().context(context).user(item.user_id));
	}

	async.filter(affectedChannels, function (channelItem, c) {
		c(channelItem.isValid());
	}, function (validChannels) {
		var channelsWithFilters = [];
		async.each(validChannels, function (channel, eachCallback) {
			tlib.subscription.getSubscriptionKeysWithFilters(channel, function (err, filteredChannels) {
				if (err) {
					err.message = 'getSubscriptionKeysWithFilters error: ' + err.message;

					return eachCallback(err);
				}

				async.each(filteredChannels, function (filteredChannel, c) {
					if (tlib.utils.testObject(item, filteredChannel.filter)) {
						channelsWithFilters.push(filteredChannel);
					}
					c();
				}, eachCallback);
			});
		}, function (err) {
			if (err) return callback(err);

			callback(null, validChannels.concat(channelsWithFilters));
		});
	});
};

AggregationWorker.prototype.validateAuthor = function (object, aclType, userId, callback) {
	let appId = object.application_id;
	if (tlib.apps.isValidModel(appId, object.type) !== true)
		return callback();
	if ((tlib.apps[appId].modelSchema(object.type).aclType & 8) && userId) {
		var authorFields = (tlib.apps[appId].modelSchema(object.type).author_fields || []).concat('user_id');

		for (var f in authorFields) {
			if (object[authorFields[f]] == userId)
				return callback();
		}

		return callback(tlib.error(tlib.errors.OperationNotAllowed));
	} else {
		callback();
	}
};

module.exports = AggregationWorker;
