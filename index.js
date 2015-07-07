var args = require('electron').argv();
var cb = require('couchbase');
var async = require('async');
var sizeof = require('object-sizeof');
var colors = require('colors');
var redis = require('redis');

var Models = require('telepat-models');
var kafka = require('./lib/kafka_client');

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
	default: {
		console.log('Invalid worker type "'+workerType+'"');
		process.exit(-1);
	}
}

var cluster = new cb.Cluster('couchbase://'+theWorker.config.couchbase.host);

async.series([
	function DataBucket(callback) {
		if (Models.Application.bucket)
			Models.Application.bucket = null;

		Models.Application.bucket = cluster.openBucket(theWorker.config.couchbase.dataBucket);
		Models.Application.bucket.on('error', function(err) {
			console.log('Failed'.bold.red+' connecting to Data Bucket on couchbase "'+theWorker.config.couchbase.host+'": '+err.message);
			console.log('Retrying...');
			setTimeout(function () {
				DataBucket(callback);
			}, 1000);
		});
		Models.Application.bucket.on('connect', function() {
			console.log('Connected to Data bucket on couchbase.'.green);
			callback();
		});
	},
	function RedisClient(callback) {
		if (Models.Application.redisClient)
			Models.Application.redisClient = null;

		Models.Application.redisClient = redis.createClient(theWorker.config.redis.port, theWorker.config.redis.host);
		Models.Application.redisClient.on('error', function(err) {
			console.log('Failed'.bold.red+' connecting to Redis "'+theWorker.config.redis.host+'": '+err.message);
			console.log('Retrying...');
		});
		Models.Application.redisClient.on('ready', function() {
			console.log('Client connected to Redis.'.green);
			callback();
		});
	},
	function KafkaClient(callback) {
		console.log('Waiting for Zookeeper connection.');
		var kafkaConfiguration = theWorker.config.kafka;
		kafkaConfiguration.topic = workerType;

		var kafkaClient = new kafka(theWorker.config.kafka.clientName+'-'+theWorker.name, kafkaConfiguration);
		theWorker.setMessagingClient(kafkaClient);

		kafkaClient.on('ready', function() {
			console.log('Client connected to Zookeeper.'.green);
			callback();
		});
		kafkaClient.on('error', function(err) {
			console.log('Kafka broker not available.'.red+' Trying to reconnect.'+err);
		});
	}
], function() {
	theWorker.ready();
});

/*
var args = require('electron').argv();
var cb = require('couchbase');
var kafka = require('kafka-node');
var async = require('async');
var sizeof = require('object-sizeof');
var colors = require('colors');
var redis = require('redis');

var Models = require('octopus-models-api');

var config = {};//require('./config.json');

if (process.env.TP_CB_HOST) {
	config.couchbase = {
		host: process.env.TP_CB_HOST,
		dataBucket: process.env.TP_CB_BUCKET,
		stateBucket: process.env.TP_CB_STATE_BUCKET
	};
} else {
	config.couchbase = require('./config.json').couchbase;
}

if (process.env.TP_KFK_HOST) {
	config.kafka = {
		host: process.env.TP_KFK_HOST,
		port: process.env.TP_KFK_PORT,
		clientName: process.env.TP_KFK_CLIENT
	};
} else {
	config.kafka = require('./config.json').kafka;
}

if (process.env.TP_REDIS_HOST) {
	config.redis = {
		host: process.env.TP_REDIS_HOST,
		port: process.env.TP_REDIS_PORT
	};
} else {
	config.redis = require('./config.json').redis;
}

var cluster = new cb.Cluster('couchbase://'+config.couchbase.host);

bucket = null;
stateBucket = null;
redisClient = null;

var topics = ['aggregation', 'write', 'track', 'update_friends'];

var Aggregator = require('./lib/aggregator');
var Writer = require('./lib/writer');
var UpdateFriends = require('./lib/update_friends');

var topic = args.params.t;
var consumerIndex = args.params.i;

if (topics.indexOf(topic) === -1 && topic.slice(-9) !== 'transport') {
    console.error(('Topic must be one of '+topics.toString()+' or a transport topic.').red);
    process.exit(-1);
}

process.title = config.kafka.clientName+'-'+topic+'-'+consumerIndex;

kafkaClient = null;
kafkaProducer = null;
kafkaConsumer = null;

process.on('SIGUSR2', function() {
    console.log('SIGUSR2');
    bucket.disconnect();
    stateBucket.disconnect();
    kafkaClient.close();
});

process.on('SIGINT', function() {
    console.log('SIGINT');
    bucket.disconnect();
    stateBucket.disconnect();
    kafkaClient.close(function() {
        process.exit(-1);
    });
});

var transport = null;

/!**
 * Forms the key of a subscription document (only for simple filters)
 * @param appId integer Application ID
 * @param context integer
 * @param item Object Item
 * @param callback Function
 * @returns string[] 5 Subscription keys (some of them but not all may be null).
 *!/
formKeys = function(appId, context, item, callback) {
	var mdl = item.type;
	var user_id = item.user_id;
	var id = item.id;

	for (var r in Models.Application.loadedAppModels[appId][mdl].belongsTo) {
		if (item[Models.Application.loadedAppModels[appId][mdl].belongsTo[r].parentModel+'_id']) {
			var parent = {model: Models.Application.loadedAppModels[appId][mdl].belongsTo[r].parentModel,
				id: item[Models.Application.loadedAppModels[appId][mdl].belongsTo[r].parentModel+'_id']};
		}
	}

	var allItemsChannelKey = 'blg:'+context+':'+Models.Application.loadedAppModels[appId][mdl].namespace;
    var userItemsChannelKey = null;
    var allChildItemsChannelKey = null;
    var userChildItemsChannelKey = null;
	var singleItemChannelKey = null;
	var result = [];

    if (user_id)
        userItemsChannelKey = allItemsChannelKey+':users:'+user_id;
    if (parent)
        allChildItemsChannelKey = allItemsChannelKey+':'+Models.Application.loadedAppModels[appId][parent.model].namespace+':'+parent.id;
    if (user_id && parent)
        userChildItemsChannelKey = userItemsChannelKey+':'+Models.Application.loadedAppModels[appId][parent.model].namespace+':'+parent.id;
	if (id)
		singleItemChannelKey = 'blg:'+Models.Application.loadedAppModels[appId][mdl].namespace+':'+id;

	var partialKeys = [allItemsChannelKey, userItemsChannelKey, allChildItemsChannelKey, userChildItemsChannelKey, singleItemChannelKey];

    allItemsChannelKey += ':deltas';
    if (userItemsChannelKey)
        userItemsChannelKey += ':deltas';
    if (allChildItemsChannelKey)
        allChildItemsChannelKey += ':deltas';
    if (userChildItemsChannelKey)
        userChildItemsChannelKey+= ':deltas';
	if (id)
		singleItemChannelKey += ':deltas';

	result = [allItemsChannelKey, userItemsChannelKey, allChildItemsChannelKey, userChildItemsChannelKey, singleItemChannelKey];

	async.each(partialKeys, function(key, c) {
		if(key) {
			var query = cb.ViewQuery.from('dev_state_document', 'by_subscription').custom({stale: false, key: '"'+key+'"'});
			stateBucket.query(query, function(err, results) {
				for(var k in results) {
					var queryObject = JSON.parse((new Buffer(results[k].value)).toString('ascii'));

					if (Models.utils.testObject(item, queryObject))
						result.push(k+':deltas');
				}
				c(err);
			});
		} else {
			c();
		}
	}, function(err) {
		if (err) return callback(err);
		callback(null, result);
	});
};

//Open connections to databases
async.series([
    function DataBucket(callback) {
        if (Models.Application.bucket)
			bucket = null;

		bucket = cluster.openBucket(config.couchbase.dataBucket);
        bucket.on('error', function(err) {
			console.log('Failed'.bold.red+' connecting to Data Bucket on couchbase "'+config.couchbase.host+'": '+err.message);
			console.log('Retrying...');
			setTimeout(function () {
				DataBucket(callback);
			}, 1000);
        });
		bucket.on('connect', function() {
			console.log('Connected to Data bucket on couchbase.'.green);
			callback();
		});
    },
    function StateBucket(callback) {
		if (stateBucket)
			stateBucket = null;

		stateBucket = cluster.openBucket(config.couchbase.stateBucket);
        stateBucket.on('error', function(err) {
			console.log('Failed'.bold.red+' connecting to State Bucket on couchbase "'+config.couchbase.host+'": '+err.message);
			console.log('Retrying...');
			setTimeout(function () {
				StateBucket(callback);
			}, 1000);
        });
		stateBucket.on('connect', function() {
			console.log('Connected to State bucket on couchbase.'.green);
			callback();
		});
    },
	function RedisClient(callback) {
		if (redisClient)
			redisClient = null;

		redisClient = redis.createClient(config.redis.port, config.redis.host);
		redisClient.on('error', function(err) {
			console.log('Failed'.bold.red+' connecting to Redis "'+config.redis.host+'": '+err.message);
			console.log('Retrying...');
		});
		redisClient.on('ready', function() {
			console.log('Client connected to Redis.'.green);
			callback();
		});
	},
	function KafkaClient(callback) {
		console.log('Waiting for Zookeeper connection.');
		kafkaClient = new kafka.Client(config.kafka.host+':'+config.kafka.port+'/', config.kafka.clientName+'-'+topic+'-'+consumerIndex);
		kafkaClient.on('ready', function() {
			console.log('Client connected to Zookeeper.'.green);

			kafkaConsumer = new kafka.HighLevelConsumer(kafkaClient, [{topic: topic}], {groupId: topic});
			kafkaConsumer.on('error', function() {});
			kafkaProducer = new kafka.HighLevelProducer(kafkaClient);
			kafkaProducer.on('error', function() {});

			callback();
		});
		kafkaClient.on('error', function(err) {
			console.log('Kafka broker not available.'.red+' Trying to reconnect.'+err);
		});
	}
], function(err) {
    Models.Application.setBucket(bucket);
    Models.Application.setStateBucket(stateBucket);

	var topicParts = topic.split('_');
	if (topicParts[1] === 'transport') {
		var transportType = topicParts[0];
		try {
			transport = require('./lib/client_transport/' + transportType);

			if (transport.initialize) {
				transport.initialize();
			}

		} catch (e) {
			if (e.code == 'MODULE_NOT_FOUND') {
				console.error(('Transport topic "'+topic+'" is not a valid transport type.').red);
				process.exit(-1);
			} else
				throw e;
		}
	}

	var functionSwitch = function(msgValue) {
		switch(topic) {
			case 'aggregation': {
				Aggregator(msgValue);

				break;
			}

			case 'write': {
				Writer(msgValue);

				break;
			}

			case 'update_friends': {
				UpdateFriends(msgValue);

				break;
			}

			default: {
				var topicParts = topic.split('_');
				if (topicParts[1] === 'transport') {
					transport.send(msgValue);
				}
			}
		}
	};

	kafkaConsumer.on('message', function(message) {
        console.log(message.value.cyan);

        var msgValue = JSON.parse(message.value);

        if (sizeof(Models.Application.loadedAppModels) > (1 << 26)) {
            delete Models.Application.loadedAppModels;
            Models.Application.loadedAppModels = {};
        }

        if (!Models.Application.loadedAppModels[msgValue.applicationId]) {
            Models.Application.loadAppModels(msgValue.applicationId, function() {
				functionSwitch(msgValue);
			});
        } else
            functionSwitch(msgValue);
    });

	var packageJson = require('./package.json');

	console.log('Telepat Worker version '+packageJson.version+' initialized at '+(new Date()).toString()+'. Queue: "'+topic+'". Consumer index: '+consumerIndex);
});
*/
