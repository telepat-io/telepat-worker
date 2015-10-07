var Models = require('telepat-models');
var async = require('async');
var sizeof = require('object-sizeof');
var microtime = require('microtime-nodejs');
var guid = require('uuid');
require('colors');

var Base_Worker = function(type, index) {
	this.type = type;
	this.index = index;
	this.config = {};
	/**
	 *
	 * @type {MessagingClient}
	 */
	this.messagingClient = null;
	this.name = type+'-'+index;

	process.title = this.name;
	this.loadConfiguration();
};

Base_Worker.OP = {
	ADD: 'add',
	UPDATE: 'update',
	DELETE: 'delete'
};

//built-in objects
Base_Worker.OBJECT_TYPE = {
	USER: 'user',
	CONTEXT: 'context'
};

Base_Worker.prototype.ready = function() {
	var self = this;
	Models.Application.loadAllApplications(function(err) {
		if (err) {
			console.log(err);
			process.exit(-1);
		}

		self.onMessage(self.processMessage.bind(self));
		var packageJson = require('../package.json');
		console.log('Telepat Worker version '+packageJson.version+' initialized at '+(new Date()).toString()+'. Queue: "'+self.type+'". Consumer index: '+self.index);
		process.on('SIGINT', self.shutdown.bind(self));
	});
};

Base_Worker.prototype.shutdown = function(callback) {
	console.log(this.name+' worker shutting down...');
	this.messagingClient.shutdown((function() {
		if (callback instanceof Function)
			callback.call(this);

		process.exit(0);
	}).bind(this));
};

/**
 *
 * @param {MessagingClient} client
 */
Base_Worker.prototype.setMessagingClient = function(client) {
	this.messagingClient = client;
};

Base_Worker.prototype.loadConfiguration = function() {
	var envVariables = {
		TP_MSG_QUE: process.env.TP_MSG_QUE,
		TP_REDIS_HOST: process.env.TP_REDIS_HOST,
		TP_REDIS_PORT: process.env.TP_REDIS_PORT,
		TP_MAIN_DB: process.env.TP_MAIN_DB
	};

	var validEnvVariables = true;
	var mainConfiguration = null;

	for(var varName in envVariables) {
		if (!envVariables[varName]) {
			console.log('Missing'.yellow+' environment variable "'+varName+'". Trying configuration file.');
			try {
				mainConfiguration = require('../config.json');
			} catch (e) {
				if (e.code == 'MODULE_NOT_FOUND') {
					console.log('Fatal error:'.red+' configuration file is missing or not accessible. Please add a configuration file from the example.');
					process.exit(-1);
				} else
					throw e;
			}

			validEnvVariables = false;
			break;
		}
	}

	if (validEnvVariables) {
		this.config.message_queue = envVariables.TP_MSG_QUE;
		this.config.redis = {
			host: envVariables.TP_REDIS_HOST,
			port: envVariables.TP_REDIS_PORT
		};
		this.config.main_database = envVariables.TP_MAIN_DB;
	} else {
		this.config = mainConfiguration;
	}
};

Base_Worker.prototype.onMessage = function(callback) {
	if (this.messagingClient) {
		this.messagingClient.onMessage(function(message) {
			var parsedMessage = JSON.parse(message);
			console.log(message.cyan);

			callback(parsedMessage);
		});
	}
};

/**
 *
 * @param {Delta} delta
 * @param callback
 */
Base_Worker.writeDelta = function(delta, callback) {
	var transaction = Models.Application.redisClient.multi();

	transaction.zadd([delta.channel.get({deltas: true}), delta.ts, JSON.stringify(delta.toObject())]);
	transaction.zcard(delta.channel.get({deltas: true}));
	transaction.exec(function(err, replies) {
		if (err)
			return callback(new Error('Transaction failed: writing delta object to key "'+delta.channel.get({deltas: true})+'" ('+err.toString()+')'));

		//returns number of deltas in the key
		callback(null, replies[1]);
	});
};

/**
 *
 * @param {Channel|String} channel
 */
Base_Worker.getAndRemoveDeltas = function(channel, callback) {
	if (channel instanceof Models.Channel)
		channel = channel.get({deltas: true});
	else
		channel += ':deltas';

	var transaction = Models.Application.redisClient.multi();

	transaction.zrange([channel, 0, -1]);
	transaction.del(channel);
	transaction.exec(function(err, replies) {
		if (err)
			return callback(new Error('Transaction failed: getting and/or removing deltas "'+channel+'" ('+err.toString()+')'));

		callback(null, replies[0]);
	});
};

/**
 *
 * @param {Channel[]|String[]} channels
 * @param appId
 * @param callback
 */
Base_Worker.multiGetAndRemoveDeltas = function(channels, appId, callback) {
	async.map(channels, function(channel, c) {
		if (channel instanceof Models.Channel)
			c(null, channel.get({deltas: true}));
		else
			c(null, channel+':deltas');
	}, function(err, deltaKeys) {
		var transaction = Models.Application.redisClient.multi();

		deltaKeys.forEach(function(deltaKey) {
			transaction.zrange([deltaKey, 0, -1]);
		});

		transaction.del(deltaKeys);
		transaction.exec(function(err, replies) {
			if (err)
				return callback(new Error('Transaction failed: getting and/or removing multiple deltas ('+err.toString()+')'));

			var deltaObjects = [];
			var operationIds = {};

			replies.slice(0, deltaKeys.length).forEach(function(deltasArray, index) {
				//injectin sub key into patches
				//var parsedResponse = replies[index];
				var transportMessageTimestamp = microtime.now();

				deltasArray.forEach(function(patchValue, patchIndex, patchesArray) {
					patchesArray[patchIndex] = JSON.parse(patchValue);
					patchesArray[patchIndex].subscription = deltaKeys[index].replace(':deltas', '');

					if (['replace', 'increment', 'append'].indexOf(patchesArray[patchIndex].op) !== -1) {
						var pathParts = patchesArray[patchIndex].path.split('/'); //modelname/id/fieldname
						var transportMessageKey = 'blg:'+appId+':'+pathParts.join(':')+':transport_msg_timestamp';
						patchesArray[patchIndex]._microtime = transportMessageTimestamp;

						//we don't want to unnecessary writes to the same key with the same value
						//because the same operation can be in multiple subscriptions
						if (!operationIds[patchesArray[patchIndex].guid]) {
							Models.Application.redisClient.set(transportMessageKey, transportMessageTimestamp, function(err, result) {
								Models.Application.redisClient.expire(transportMessageKey, 60);
							});
						}
						operationIds[patchesArray[patchIndex].guid] = true;
					}
				});

				deltaObjects = deltaObjects.concat(deltasArray);
			});

			callback(null, deltaObjects);
		});
	});
};

Base_Worker.prototype.getAffectedChannels = function(item, appId, callback) {
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
				if (err) return eachCallback(err);

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

Base_Worker.prototype.processMessage = function(message) {
	throw new Error('Unimplemented method, processMessage');
};

Base_Worker.createDelta = function(op, value, path) {
	return new Models.Delta(op, value, path, null, guid.v4(), process.hrtime().join(''));
};

module.exports = Base_Worker;

