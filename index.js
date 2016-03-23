var args = require('electron').argv();
var async = require('async');
colors = require('colors');
var redis = require('redis');

var Models = require('telepat-models');

var workerType = args.params.t;
var workerIndex = args.params.i;
/**
 *
 * @type {Base_Worker}
 */
var theWorker = null;

switch (workerType) {
	case 'aggregation':	{
		var AggregationWorker = require('./lib/aggregation_worker');
		theWorker = new AggregationWorker(workerIndex);

		break;
	}
	case 'write': {
		var WriterWorker = require('./lib/writer_worker');
		theWorker = new WriterWorker(workerIndex);

		break;
	}
	case 'update_friends': {
		var UpdateFriendsWorker = require('./lib/update_friends_worker');
		theWorker = new UpdateFriendsWorker(workerIndex);

		break;
	}
	default: {
		var workerTypeParts = workerType.split('_');
		if (workerTypeParts[1] === 'transport') {
			var ClientTransportWorker = require('./lib/client_transport/'+workerTypeParts[0]);
			theWorker = new ClientTransportWorker(workerIndex);
		} else {
			console.log('Invalid worker type "'+workerType+'"');
			process.exit(-1);
		}
	}
}

theWorker.config.subscribe_limit = theWorker.config.subscribe_limit || 64;
theWorker.config.get_limit = theWorker.config.get_limit || 384;

if (theWorker.config.logger) {
	theWorker.config.logger.name = theWorker.name;
	Models.Application.logger = new Models.TelepatLogger(theWorker.config.logger);
} else {
	Models.Application.logger = new Models.TelepatLogger({
		type: 'Console',
		name: theWorker.name,
		settings: {level: 'info'}
	});
}

if (!Models[theWorker.config.main_database]) {
	Models.Application.logger.emergency('Unable to load "'+theWorker.config.main_database+
		'" main database: not found. Aborting...');
	process.exit(-1);
}

Models.Application.datasource = new Models.Datasource();
Models.Application.datasource.setMainDatabase(new Models[theWorker.config.main_database](theWorker.config[theWorker.config.main_database]));

async.series([
	function(callback) {
		Models.Application.datasource.dataStorage.onReady(function() {
			callback();
		});
	},
	function(callback) {
		if (Models.Application.redisClient)
			Models.Application.redisClient = null;

		Models.Application.redisClient = redis.createClient(theWorker.config.redis.port, theWorker.config.redis.host);
		Models.Application.redisClient.on('error', function(err) {
			Models.Application.logger.error('Failed connecting to Redis "'+
				theWorker.config.redis.host+'": '+err.message+'. Retrying...');
		});
		Models.Application.redisClient.on('ready', function() {
			Models.Application.logger.info('Client connected to Redis.');
			callback();
		});
	},
	function(callback) {
		if (Models.Application.redisCacheClient)
			Models.Application.redisCacheClient = null;

		Models.Application.redisCacheClient = redis.createClient(theWorker.config.redisCache.port, theWorker.config.redisCache.host);
		Models.Application.redisCacheClient.on('error', function(err) {
			Models.Application.logger.error('Failed connecting to Redis Cache "'+theWorker.config.redisCache.host+'": '+
				err.message+'. Retrying...');
		});
		Models.Application.redisCacheClient.on('ready', function() {
			Models.Application.logger.info('Client connected to Redis Cache.');
			callback();
		});
	},
	function(callback) {
		if (!Models[theWorker.config.message_queue]) {
			Models.Application.logger.emergency('Unable to load "'+theWorker.config.message_queue+
				'" messaging queue: not found. Aborting...');
			process.exit(-1);
		}

		var messageQueueConfig = theWorker.config[theWorker.config.message_queue];

		if (messageQueueConfig === undefined) {
			messageQueueConfig = {broadcast: theWorker.broadcast, exclusive: theWorker.exclusive};
		} else {
			messageQueueConfig.broadcast = theWorker.broadcast;
			messageQueueConfig.exclusive = theWorker.exclusive;
		}

		var messagingClient = new Models[theWorker.config.message_queue](messageQueueConfig, theWorker.name, workerType);
		theWorker.setMessagingClient(messagingClient);

		messagingClient.onReady(callback);
	}
], function() {
	theWorker.ready();
});
