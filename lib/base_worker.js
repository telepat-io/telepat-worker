var tlib = require('telepat-models');
var async = require('async');
var aws = require('aws-sdk');
var guid = require('uuid');
var tlib = require('telepat-models');

/**
 * @typedef {{
 *		op: Delta.OP,
 *		object: Object,
 *		application_id: string,
 *		timestamp: Number,
 *		[patches]: Patch[],
 *		[instant]: Boolean
 * }} AggregatorMessage
 */

/**
 * @typedef {{
 *		key: string[]
 * }} WriterMessage
 */

/**
 * @typedef {{
 *		deltas: {
 *			op: Delta.OP,
 *			object: Object,
 *			subscriptions: string[],
 *			application_id: string,
 *			timestamp: Number,
 *			[patch]: Patch[]
 *		}[],
 *		_broadcast: Boolean
 * }} TransportManagerMessage
 */

/**
 * @typedef {{
 * 		deviceTokens: string[],
 *		deltas: {
 *			op: Delta.OP,
 *			object: Object,
 *			subscriptions: string[],
 *			application_id: string,
 *			timestamp: Number,
 *			[patch]: Patch[]
 *		}[],
 *		applicationId: string
 * }} Payload
 */

/**
 * @typedef {{
 *		payload: Object.<string, Payload>
 * }} ClientTransportMessage
 */

/**
 * Used for storing notifications with a single delta that exceeds the payload limit
 * @type {Object}
 */
var amazonS3Services = {};

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
	self.onMessage(self.processMessage.bind(self));
	var packageJson = require('../package.json');

	tlib.services.logger.info('Telepat Worker version '+packageJson.version+' initialized at '
			+(new Date()).toString()+'. Queue: "'+self.type+'". Consumer index: '+self.index);
	process.on('SIGINT', self.shutdown.bind(self));
	
};

/**
 * This function should be called before the process is terminated
 * @param callback Called after all services has been shut down
 */
Base_Worker.prototype.shutdown = function(callback) {
	tlib.services.logger.info(this.name+' worker shutting down...');
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
		tlib.SystemMessageProcessor.identity = this.name;
		this.messagingClient.onMessage(function(message) {
			var parsedMessage = JSON.parse(message);
			
			if (parsedMessage._systemMessage) {
				tlib.services.logger.debug('Got system message: "'+message+'"');
				tlib.SystemMessageProcessor.process(parsedMessage);
			} else {
				tlib.services.logger.debug('Got message: "'+message+'"');
				callback(parsedMessage);
			}
		});
	}
};

/**
 * A function that should be implemented where the message is processed
 * @abstract
 */
Base_Worker.prototype.processMessage = function(message) {
	throw new Error('Unimplemented method, processMessage');
};

/**
 * @callback sendAmazonLinkCb
 * @param {Error|null} [err]
 * @param {string} [publicLink]
 */
/**
 *
 * @param {Object} payload
 * @param {Array} payload.new
 * @param {Array} payload.updated
 * @param {Array} payload.deleted
 * @param {sendAmazonLinkCb} callback
 */
Base_Worker.prototype.storeAmazonLink = function(payload, callback) {
	var applicationId = null;

	if (payload.new.length)
		applicationId = payload.new[0].application_id;
	else if (payload.updated.length)
		applicationId = payload.updated[0].application_id;
	else if (payload.deleted.length)
		applicationId = payload.deleted[0].application_id;

	var app = tlib.apps[applicationId];

	if (!app.amazon.access_key_id || !app.amazon.secret_access_key || !app.amazon.region || !app.amazon.bucket) {
		return callback(new Error('Unable to send notification through Amazon S3. Application not configured'));
	}

	if (!amazonS3Services[app.id]) {
		amazonS3Services[app.id] = new aws.S3({
			accessKeyId: app.amazon.access_key_id,
			secretAccessKey: app.amazon.secret_access_key,
			region: app.amazon.region
		});
	} else {
		var changed = (app.amazon.access_key_id !== amazonS3Services[app.id].accessKeyId) ||
			(app.amazon.secret_access_key !== amazonS3Services[app.id].secretAccessKey) ||
			(app.amazon.region !== amazonS3Services[app.id].region);

		if (changed) {
			amazonS3Services[app.id] = new aws.S3({
				accessKeyId: app.amazon.access_key_id,
				secretAccessKey: app.amazon.secret_access_key,
				region: app.amazon.region
			});
		}
	}

	var resourceId = guid.v4();

	amazonS3Services[app.id].putObject({
		Bucket: app.amazon.bucket,
		Key: resourceId,
		Body: JSON.stringify(payload),
		Expires: parseInt(Date.now()/1000)+86400,
		ContentType: 'application/json',
		ContentEncoding: 'utf-8',
		ACL: 'public-read'
	}, function(err) {
		if (err)
			return callback(err);

		callback(null, 'https://s3.'+app.amazon.region+'.amazonaws.com/'+app.amazon.bucket+'/'+resourceId);
	});
};

module.exports = Base_Worker;

