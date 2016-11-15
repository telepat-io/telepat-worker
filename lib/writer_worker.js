var Base_Worker = require('./base_worker');
var Models = require('telepat-models');
var async = require('async');

/**
 *
 * @param {int} index Index of the write worker. Write workers consume from a common message queue
 * @constructor
 */
var WriterWorker = function(index) {
	Base_Worker.call(this, 'write', index);
	/**
	 *
	 * @type {int} The current size of deltas to fetch from Redis. Starts at 1 and doubles everytime there are still
	 * deltas left after retrieving a batch of deltas. Limited at 512.
	 */
	this.fetchCount = 1;
};

WriterWorker.prototype = Object.create(Base_Worker.prototype);

/**
 *
 * @param {Object} message
 * @param {string} message.key The redis key where the deltas are stored
 */
WriterWorker.prototype.processMessage = function(message) {
	var self = this;
	var deltas = [];
	var remainingDeltas = false;

	async.series([
		function getDeltas(callback) {
			self.getAndRemoveDeltas(message.key, function(err, results, remaining) {
				if (err) return callback(err);

				remainingDeltas = !!remaining;

				if (!remaining && self.fetchCount > 1)
					self.fetchCount /= 2;
				else if (remaining && self.fetchCount < 512) {
					remainingDeltas = true;
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

		if (remainingDeltas)
			self.messagingClient.send([JSON.stringify({key: message.key})], 'write', function(err) {
				if (err) {
					Models.Application.logger.error('Failed to send message to writers: '+err.message);
				} else {
					Models.Application.logger.debug('Sent additional message to writers to grab deltas (fetchCount at '+self.fetchCount+')');
				}
			});
	});
};

/**
 * @callback getAndRemoveDeltasCb
 * @param {Error|null} err
 * @param {Object[]} [deltas]
 * @param {int} [remaining] The number of remaining deltas after fetch
 */
/**
 *
 * @param {string} redisKey The redis key where the deltas are stored
 * @param {getAndRemoveDeltasCb} callback
 */
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
 * @callback processDeltasCb
 * @param {Error|null} err
 * @param {Object[]} deltas The 'create' delta's object is complete with ID and everything
 */
/**
 *
 * @param {Object[]} deltas
 * @param {processDeltasCb} callback
 */
WriterWorker.prototype.processDeltas = function(deltas, callback) {
	var createDeltas = [];
	var updatePatches = [];
	var updateDeltaPatches = [];
	var deleteDeltaObjects = [];
	var deleteObjects = {};

	async.each(deltas, function(delta, c) {
		switch(delta.op) {
			case 'create':
				createDeltas.push(delta);

				break;
			case 'update':
				if (!delta.instant)
					updatePatches.push(delta.patch);
				else
					delete delta.instant;
				updateDeltaPatches.push(delta);

				break;
			case 'delete':
				if (delta.object.type == 'user') {
					Models.User.delete(delta.object.id, delta.object.application_id, function() {});
				} else
					deleteObjects[delta.object.id] = delta.object.type;

				deleteDeltaObjects.push(delta);

				break;
		}
		c();
	}, function() {
		var created = [];

		async.series([
			function(callback1) {
				if (updatePatches.length) {
					Models.Model.update(updatePatches, function(err, modified) {
						if (err) return Models.Application.logger.error('Error processing update requests: '+err.message);

						updateDeltaPatches = updateDeltaPatches.filter(function(delta) {
							return !!modified[delta.patch.path.split('/')[1]];
						});

						callback1();
					});
				} else {
					callback1();
				}
			},
			function(callback1) {
				if (createDeltas.length) {
					Models.Model.create(createDeltas, function(err) {
						if (err) Models.Application.logger.error('Error processing create requests: '+err.message+'\n'+err.stack);
					}, function(createdItems) {
						created = createdItems;
						callback1();
					});
				} else {
					callback1();
				}
			}
		], function() {
			if (Object.keys(deleteObjects).length) {
				Models.Model.delete(deleteObjects, function(err) {
					if (err) Models.Application.logger.error('Error processing update requests: '+err.message);
				});
			}

			callback(null, created.concat(updateDeltaPatches, deleteDeltaObjects));
		});
	});
};

module.exports = WriterWorker;
