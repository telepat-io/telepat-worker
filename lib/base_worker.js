var Models = require('telepat-models');
var async = require('async');
require('colors');

/**
 *
 * @param {string} type One of: aggregation, write, transport_manager, sockets_transport, ios_transport, android_transport
 * @param {int} index The index of the worker that will be assigned. This is important for volatile transports, as they
 * have their own message queue.
 * @constructor
 */
var Base_Worker = function(type, index, config) {
	this.type = type;
	this.index = index;
	this.config = config;
	this.broadcast = false;
	/**
	 *
	 * @type {MessagingClient}
	 */
	this.messagingClient = null;
	this.name = type+'-'+index;

	process.title = this.name;
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

/**
 * Should be called when all dependand services are connected to
 */
Base_Worker.prototype.ready = function() {
	var self = this;
	Models.Application.loadAllApplications(null, null, function(err) {
		if (err) {
			Models.Application.logger.emergency('Couldn\'t load applications: '+err.message);
			process.exit(1);
		}

		self.onMessage(self.processMessage.bind(self));
		var packageJson = require('../package.json');
		Models.Application.logger.info('Telepat Worker version '+packageJson.version+' initialized at '
			+(new Date()).toString()+'. Queue: "'+self.type+'". Consumer index: '+self.index);
		process.on('SIGINT', self.shutdown.bind(self));
	});
};

/**
 * This function should be called before the process is terminated
 * @param callback Called after all services has been shut down
 */
Base_Worker.prototype.shutdown = function(callback) {
	Models.Application.logger.info(this.name+' worker shutting down...');
	this.messagingClient.shutdown((function() {
		if (callback instanceof Function)
			callback.call(this);

		process.exit(0);
	}).bind(this));
};

/**
 * @callback OnMessageCb
 * @param {Object} message An object representing the message coming from another worker or API
 */
/**
 * Sets a function that processes a message arriving from the message queue
 * @param {OnMessageCb} callback
 */
Base_Worker.prototype.onMessage = function(callback) {
	if (this.messagingClient) {
		Models.SystemMessageProcessor.identity = this.name;
		this.messagingClient.onMessage(function(message) {
			var parsedMessage = JSON.parse(message);

			if (parsedMessage._systemMessage) {
				Models.Application.logger.debug('Got system message: "'+message+'"');
				Models.SystemMessageProcessor.process(parsedMessage);
			} else {
				Models.Application.logger.debug('Got message: "'+message+'"');
				callback(parsedMessage);
			}
		});
	}
};

/**
 * A function that should be implemented where the message is processed
 * @abstract
 * @param {Object} message
 */
Base_Worker.prototype.processMessage = function(message) {
	throw new Error('Unimplemented method, processMessage');
};

module.exports = Base_Worker;

