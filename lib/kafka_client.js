var MessagingClient = require('./messaging_client');
var kafka = require('kafka-node');

var KafkaClient = function(name, config){
	MessagingClient.call(this, name, config);
	this.connectionClient = kafka.Client(config.host+':'+config.port+'/', name);

	this.kafkaConsumer = new kafka.HighLevelConsumer(this.connectionClient, [{topic: config.topic}], {groupId: config.topic});
	this.kafkaConsumer.on('error', function() {});
	this.kafkaProducer = new kafka.HighLevelProducer(this.connectionClient);
	this.kafkaProducer.on('error', function() {});
};

KafkaClient.prototype = MessagingClient.prototype;

KafkaClient.prototype.send = function(message, callback) {
	this.kafkaProducer.send(message, callback);
};

KafkaClient.prototype.onMessage = function(callback) {
	this.kafkaConsumer.on('message', callback);
};

KafkaClient.prototype.shutdown = function(callback) {
	this.connectionClient.close(callback);
};

KafkaClient.prototype.consumerOn = function(event, callback) {
	this.kafkaConsumer.on(event, callback);
};

KafkaClient.prototype.producerOn = function(event, callback) {
	this.kafkaProducer.on(event, callback);
};

module.exports = KafkaClient;
