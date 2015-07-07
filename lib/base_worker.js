var Models = require('telepat-models');
var async = require('async');
var cb = require('couchbase');
var sizeof = require('object-sizeof');
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

Base_Worker.prototype.ready = function() {
	this.onMessage(this.processMessage.bind(this));
	var packageJson = require('../package.json');
	console.log('Telepat Worker version '+packageJson.version+' initialized at '+(new Date()).toString()+'. Queue: "'+this.type+'". Consumer index: '+this.index);

	process.on('SIGINT', this.shutdown.bind(this));
};

Base_Worker.prototype.shutdown = function() {
	console.log(this.name+' worker shutting down...');
	this.messagingClient.shutdown(function() {
		Models.Application.bucket.disconnect();
		process.exit(0);
	});
};

/**
 *
 * @param {MessagingClient} client
 */
Base_Worker.prototype.setMessagingClient = function(client) {
	this.messagingClient = client;
};

Base_Worker.prototype.loadConfiguration = function() {
	if (process.env.TP_CB_HOST) {
		this.config.couchbase = {
			host: process.env.TP_CB_HOST,
			dataBucket: process.env.TP_CB_BUCKET,
			stateBucket: process.env.TP_CB_STATE_BUCKET
		};
	} else {
		this.config.couchbase = require('../config.json').couchbase;
	}

	if (process.env.TP_KFK_HOST) {
		this.config.kafka = {
			host: process.env.TP_KFK_HOST,
			port: process.env.TP_KFK_PORT,
			clientName: process.env.TP_KFK_CLIENT
		};
	} else {
		this.config.kafka = require('../config.json').kafka;
	}

	if (process.env.TP_REDIS_HOST) {
		this.config.config.redis = {
			host: process.env.TP_REDIS_HOST,
			port: process.env.TP_REDIS_PORT
		};
	} else {
		this.config.redis = require('../config.json').redis;
	}
};

Base_Worker.prototype.onMessage = function(callback) {
	if (this.messagingClient) {
		this.messagingClient.onMessage(function(message) {
			var parsedMessage = JSON.parse(message.value);
			console.log(message.value.cyan);

			if (sizeof(Models.Application.loadedAppModels) > (1 << 26)) {
				delete Models.Application.loadedAppModels;
				Models.Application.loadedAppModels = {};
			}

			if (!Models.Application.loadedAppModels[parsedMessage.applicationId]) {
				Models.Application.loadAppModels(parsedMessage.applicationId, function() {
					callback(parsedMessage);
				});
			} else
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
			return callback(new Error('Transaction failed: writing delta object to key '+delta.channel.get({deltas: true})));

		//returns number of deltas in the key
		callback(null, replies[1]);
	});
};

Base_Worker.prototype.getAffectedChannels = function(item, callback) {
	var context = item.context_id;
	var appId = item.application_id;
	var mdl = item.type;
	var parent = {};
	var affectedChannels = [];
	var modelsBaseChannel = (new Models.Channel(appId)).model(item.type);
	affectedChannels.push(modelsBaseChannel);

	var _this = this;

	//the channel of one object
	affectedChannels.push((new Models.Channel(appId)).model(item.type, item.id));
	//all objects of type  from context
	affectedChannels.push(modelsBaseChannel.clone().context(context));

	var modelSchema = Models.Application.loadedAppModels[appId][mdl];
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
			Models.Subscription.getSubscriptionKeysWithFilters(channel, function(err, result) {
				if (err) return eachCallback(err);

				//todo: verify that item passes channel filter
				channelsWithFilters = channelsWithFilters.concat(result);
				eachCallback();
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
	return new Models.Delta(op, value, path);
};

module.exports = Base_Worker;

