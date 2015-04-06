var args = require('electron').argv();
var cb = require('couchbase');
var kafka = require('kafka-node');

var Models = require('octopus-models-api');

var config = require('./config.json');

var cluster = new cb.Cluster('couchbase://'+config.couchbase.host);
bucket = cluster.openBucket(config.couchbase.dataBucket);
stateBucket = cluster.openBucket(config.couchbase.stateBucket);

Models.Application.setBucket(bucket);
Models.Application.setStateBucket(stateBucket);
Models.Model._spec = Models.getModels();

var topics = ['aggregation', 'write', 'track'];

var Aggregator = require('./lib/aggregator');
var Writer = require('./lib/writer');

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
    bucket.disconnect();
    stateBucket.disconnect();
    kafkaClient.close(function() {
        process.exit(-1);
    });
});

//process.on('beforeExit', close);
//process.on('uncaughtException', close);

kafkaConsumer.on('error', function(err) {
    console.log(err);
});

formKeys = function(mdl, context, user_id, parent) {
    var allItemsChannelKey = 'blg:'+context+':'+Models.Model._spec[mdl].namespace;
    var userItemsChannelKey = null;
    var allChildItemsChannelKey = null;
    var userChildItemsChannelKey = null;

    console.log(parent);

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

bucket.on('connect', function() {
    kafkaConsumer.on('message', function(message) {
        console.log(message.value);

        var msgValue = JSON.parse(message.value);

        switch(topic) {
            case 'aggregation': {
                Aggregator(msgValue);

                break;
            }

            case 'write': {
                Writer(msgValue);

                break;
            }
        }
    });
});

bucket.on('error', function(err) {
    console.log(err);
});