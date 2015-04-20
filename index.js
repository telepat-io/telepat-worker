var args = require('electron').argv();
var cb = require('couchbase');
var kafka = require('kafka-node');
var async = require('async');
var sizeof = require('object-sizeof');

var Models = require('octopus-models-api');

var config = require('./config.json');

var cluster = new cb.Cluster('couchbase://'+config.couchbase.host);

bucket = null;
stateBucket = null;
opIdentifiersBucket = null;

var topics = ['aggregation', 'write', 'track', 'update_friends'];

var Aggregator = require('./lib/aggregator');
var Writer = require('./lib/writer');
var UpdateFriends = require('./lib/update_friends');

var topic = args.params.t;
var consumerIndex = args.params.i;

if (topics.indexOf(topic) === -1) {
    console.error('Topic must be one of '+topics.toString());
    process.exit(-1);
}

process.title = config.kafka.clientName+'-'+topic+'-'+consumerIndex;

var kafkaClient = new kafka.Client(config.kafka.host+':'+config.kafka.port+'/', config.kafka.clientName+'-'+topic+'-'+consumerIndex);
kafkaConsumer = new kafka.HighLevelConsumer(kafkaClient, [{topic: topic}], {groupId: topic});
kafkaProducer = new kafka.HighLevelProducer(kafkaClient);

/*function close() {
    kafkaClient.close(true, function() {
        process.exit(-1);
    });
}*/

process.on('SIGUSR2', function() {
    console.log('SIGUSR2');
    bucket.disconnect();
    stateBucket.disconnect();
    opIdentifiersBucket.disconnect();
    kafkaClient.close();
});

process.on('SIGTERM', function() {
    console.log('SIGTERM');
    bucket.disconnect();
    stateBucket.disconnect();
    opIdentifiersBucket.disconnect();
    kafkaClient.close(function() {
        process.exit(-1);
    });
});

process.on('exit', function() {
    console.log('EXIT');
    if (bucket)
        bucket.disconnect();
    if (stateBucket)
        stateBucket.disconnect();
    if (opIdentifiersBucket)
        opIdentifiersBucket.disconnect();
    kafkaClient.close();
});

//process.on('beforeExit', close);
//process.on('uncaughtException', close);

kafkaConsumer.on('error', function(err) {
    console.log(err);
    bucket.disconnect();
    stateBucket.disconnect();
    opIdentifiersBucket.disconnect();
    process.exit(-1);
});

formKeys = function(mdl, context, user_id, parent) {
    var allItemsChannelKey = 'blg:'+context+':'+Models.Model._spec[mdl].namespace;
    var userItemsChannelKey = null;
    var allChildItemsChannelKey = null;
    var userChildItemsChannelKey = null;

    if (user_id)
        userItemsChannelKey = allItemsChannelKey+':users:'+user_id;
    if (parent)
        allChildItemsChannelKey = allItemsChannelKey+':'+Models.Model._spec[parent.model].namespace+':'+parent.id;
    if (user_id && parent)
        userChildItemsChannelKey = userItemsChannelKey+':'+Models.Model._spec[parent.model].namespace+':'+parent.id;

    allItemsChannelKey += ':deltas';
    if (userItemsChannelKey)
        userItemsChannelKey += ':deltas';
    if (allChildItemsChannelKey)
        allChildItemsChannelKey += ':deltas';
    if (userChildItemsChannelKey)
        userChildItemsChannelKey+= ':deltas';

    return [allItemsChannelKey, userItemsChannelKey, allChildItemsChannelKey, userChildItemsChannelKey];
};

async.series([
    function(callback) {
        bucket = cluster.openBucket(config.couchbase.dataBucket);
        bucket.nodeConnectionTimeout = 10000;
        bucket.on('connect', callback);
        bucket.on('error', function(err) {
            console.log('Data bucket error: ', err);
            bucket.disconnect();
            process.exit(-1);
        });
    },
    function (callback) {
        stateBucket = cluster.openBucket(config.couchbase.stateBucket);
        stateBucket.nodeConnectionTimeout = 10000;
        stateBucket.on('connect', callback);
        stateBucket.on('error', function(err) {
            console.log('State bucket error: ', err);
            bucket.disconnect();
            process.exit(-1);
        });
    },
    function (callback) {
        opIdentifiersBucket = cluster.openBucket(config.couchbase.opIdentifierBucket);
        opIdentifiersBucket.nodeConnectionTimeout = 10000;
        opIdentifiersBucket.on('connect', callback);
        opIdentifiersBucket.on('error', function(err) {
            console.log('Op identifiers bucket error: ', err);
            bucket.disconnect();
            stateBucket.disconnect();
            process.exit(-1);
        });
    }
], function(err) {
    Models.Application.setBucket(bucket);
    Models.Application.setStateBucket(stateBucket);

    kafkaConsumer.on('message', function(message) {
        console.log(message.value);

        var msgValue = JSON.parse(message.value);

        var functionSwitch = function() {
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
            }
        };

        if (sizeof(Models.Application.loadedAppModels) > (1 << 26)) {
            delete Models.Application.loadedAppModels;
            Models.Application.loadedAppModels = {};
        }

        if (!Models.Application.loadedAppModels[msgValue.applicationId]) {
            Models.Application.loadAppModels(msgValue.applicationId, functionSwitch);
        } else
            functionSwitch();
    });
});
