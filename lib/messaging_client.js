var MessagingClient = function(name, config) {
	this.config = config;
	this.connectionClient = null;
};

MessagingClient.prototype.onMessage = function(callback) {
	throw new Error('Unimplemented onMessage function.');
};

/**
 * @override
 * @param message
 * @param opts
 */
MessagingClient.prototype.send = function(message, opts) {
	throw new Error('Unimplemented send function.');
};

MessagingClient.prototype.shutdown = function(callback) {
	throw new Error('Unimplemented shutdown function.');
};

MessagingClient.prototype.getName = function() {
	return this.config.name;
};

MessagingClient.prototype.on = function(event, callback) {
	if (typeof this.connectionClient.on == 'function')
		this.connectionClient.on(event, callback);
};

module.exports = MessagingClient;
