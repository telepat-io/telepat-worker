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
var Base_Worker = function(type, index) {
	this.type = type;
	this.index = index;
	this.config = {};
	this.broadcast = false;
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
 * Loads configuration from file or environment variables and processes them into the config instance variable
 */
Base_Worker.prototype.loadConfiguration = function() {
	var envVariables = {
		TP_MSG_QUE: process.env.TP_MSG_QUE,
		TP_REDIS_HOST: process.env.TP_REDIS_HOST,
		TP_REDIS_PORT: process.env.TP_REDIS_PORT,
		TP_REDISCACHE_HOST: process.env.TP_REDISCACHE_HOST || process.env.TP_REDIS_HOST ,
		TP_REDISCACHE_PORT: process.env.TP_REDISCACHE_PORT || process.env.TP_REDIS_PORT,
		TP_MAIN_DB: process.env.TP_MAIN_DB,
		TP_LOGGER: process.env.TP_LOGGER,
		TP_LOG_LEVEL: process.env.TP_LOG_LEVEL
	};

	this.config = {
		main_database: envVariables.TP_MAIN_DB,
		message_queue: envVariables.TP_MSG_QUE,
		logger: {
			type: envVariables.TP_LOGGER,
			settings: {
				level: envVariables.TP_LOG_LEVEL
			}
		},
		redis: {
			host: envVariables.TP_REDIS_HOST,
			port: envVariables.TP_REDIS_PORT
		},
		redisCache: {
			host: envVariables.TP_REDISCACHE_HOST,
			port: envVariables.TP_REDISCACHE_PORT
		}
	};

	for(var varName in envVariables) {
		if (!envVariables[varName]) {
			console.log('Missing'.yellow+' environment variable "'+varName+'". Trying configuration file.');
			try {
				this.config = require('../config.json');
			} catch (e) {
				if (e.code == 'MODULE_NOT_FOUND') {
					console.log('Fatal error:'.red+' configuration file is missing or not accessible. Please add a configuration file from the example.');
					process.exit(-1);
				} else
					throw e;
			}

			break;
		}
	}
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
		this.messagingClient.onMessage(function(message) {
			var parsedMessage = JSON.parse(message);
			Models.Application.logger.debug('Got message: "'+message+'"');

			callback(parsedMessage);
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

