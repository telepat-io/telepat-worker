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

if (Models[theWorker.config.logger]) {
	Models.Application.logger = new Models[theWorker.config.logger]('telepat_'+theWorker.name,
		theWorker.config[theWorker.config.logger]);
} else {
	Models.Application.logger = new Models['console_logger']('telepat_'+theWorker.name);
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
		if (!Models[theWorker.config.message_queue]) {
			Models.Application.logger.emergency('Unable to load "'+theWorker.config.message_queue+
				'" messaging queue: not found. Aborting...');
			process.exit(-1);
		}

		var messageQueueConfig = theWorker.config[theWorker.config.message_queue];

		if (messageQueueConfig === undefined) {
			messageQueueConfig = {broadcast: theWorker.broadcast};
		} else {
			messageQueueConfig.broadcast = theWorker.broadcast;
		}

		var messagingClient = new Models[theWorker.config.message_queue](messageQueueConfig, 'telepat-worker-'+workerType+'-'+workerIndex, workerType);
		theWorker.setMessagingClient(messagingClient);

		messagingClient.onReady(callback);
	}
], function() {
	theWorker.ready();
});
