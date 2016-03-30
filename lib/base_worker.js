var Models = require('telepat-models');
var async = require('async');
var microtime = require('microtime-nodejs');
var guid = require('uuid');
require('colors');

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

Base_Worker.prototype.ready = function() {
	var self = this;
	Models.Application.loadAllApplications(null, null, function(err) {
		if (err) {
			Models.Application.logger.emergency('Couldn\'t load applications: '+err.message);
			process.exit(-1);
		}

		self.onMessage(self.processMessage.bind(self));
		var packageJson = require('../package.json');
		Models.Application.logger.info('Telepat Worker version '+packageJson.version+' initialized at '
			+(new Date()).toString()+'. Queue: "'+self.type+'". Consumer index: '+self.index);
		process.on('SIGINT', self.shutdown.bind(self));
	});
};

Base_Worker.prototype.shutdown = function(callback) {
	Models.Application.logger.info(this.name+' worker shutting down...');
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

Base_Worker.prototype.onMessage = function(callback) {
	if (this.messagingClient) {
		this.messagingClient.onMessage(function(message) {
			var parsedMessage = JSON.parse(message);
			Models.Application.logger.info('Got message: "'+message+'"');

			callback(parsedMessage);
		});
	}
};

Base_Worker.prototype.processMessage = function(message) {
	throw new Error('Unimplemented method, processMessage');
};

Base_Worker.createDelta = function(op, value, path) {
	return new Models.Delta(op, value, path, null, guid.v4(), process.hrtime().join(''));
};

module.exports = Base_Worker;

