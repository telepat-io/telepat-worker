var Base_Worker = require('./base_worker');
var Models = require('telepat-models');
var async = require('async');

var WriterWorker = function(index) {
	Base_Worker.call(this, 'write', index);
	this.fetchCount = 1;
};

WriterWorker.prototype = Object.create(Base_Worker.prototype);

WriterWorker.prototype.processMessage = function(message) {
	var self = this;
	var deltas = [];

	async.series([
		function getDeltas(callback) {
			self.getAndRemoveDeltas(message.key, function(err, results, remaining) {
				if (err) return callback(err);

				if (!remaining && self.fetchCount > 1)
					self.fetchCount /= 2;
				else if (remaining && self.fetchCount < 512) {
					self.messagingClient.send([JSON.stringify({key: message.key})], 'write', function(err) {
						if (err) {
							Models.Application.logger.error('Failed to send message to writers: '+err.message);
						} else {
							Models.Application.logger.debug('Sent additional message to writers to grab deltas');
						}
					});
					self.fetchCount *= 2;
				}

				deltas = results;
				callback();
			})
		},
		function processDeltas(callback) {
			self.processDeltas(deltas, function(err, results) {
				if (err) return callback(err);

				deltas = results;
				callback();
			});
		},
		function notifyTransportManager(callback) {
			self.messagingClient.send([JSON.stringify({deltas: deltas})], 'transport_manager', function(err) {
				if (err) {
					Models.Application.logger.error('Failed to send message to transport_manager: '+err.message);
				}
				callback();
			});
		}
	], function(err) {
		if (err)
			Models.Application.logger.error(err.toString());
	});
};

WriterWorker.prototype.getAndRemoveDeltas = function(redisKey, callback) {
	var deltas = [];
	var transaction = Models.Application.redisClient.multi();

	transaction.zrange([redisKey, 0, this.fetchCount-1]);
	transaction.zremrangebyrank([redisKey, 0, this.fetchCount-1]);
	transaction.zcard([redisKey]);

	transaction.exec(function(err, replies) {
		if (err)
			return callback(new Error('Failed to get deltas: '+err.message));

		async.each(replies[0], function(r, c) {
			deltas.push(JSON.parse(r));
			c();
		}, function() {
			callback(null, deltas, replies[2]);
		});
	});
};

/**
 *
 * @param deltas
 * @param callback
 */
WriterWorker.prototype.processDeltas = function(deltas, callback) {
	var createDeltas = [];
	var updatePatches = [];
	var deleteDeltaObjects = [];
	var deleteObjects = {};

	async.each(deltas, function(delta, c) {
		switch(delta.op) {
			case 'create':
				createDeltas.push(delta);

				break;
			case 'update':
				updatePatches.push(delta.patch);

				break;
			case 'delete':
				deleteObjects[delta.object.id] = delta.object.type;
				deleteDeltaObjects.push(delta);

				break;
		}
		c();
	}, function() {
		if (createDeltas.length) {
			Models.Model.create(createDeltas, function(err) {
				if (err) Models.Application.logger.error('Error processing create requests: '+err.message+'\n'+err.stack);
			}, function(createdItems) {
				callback(null, createdItems.concat(updatePatches, deleteDeltaObjects));
			});
		}
		if (updatePatches.length) {
			Models.Model.update(updatePatches, function(err) {
				if (err) Models.Application.logger.error('Error processing update requests: '+err.message);
			})
		}
		if (Object.keys(deleteObjects).length) {
			Models.Model.delete(deleteObjects, function(err) {
				if (err) Models.Application.logger.error('Error processing update requests: '+err.message);
			});
		}

		if (!createDeltas.length)
			callback(null, []);
	});
};

module.exports = WriterWorker;
