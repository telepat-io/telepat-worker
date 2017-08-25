var Base_Worker = require('./base_worker');
var tlib = require('telepat-models');
var async = require('async');

/**
 *
 * @param {int} index Index of the write worker. Write workers consume from a common message queue
 * @constructor
 */
var WriterWorker = function (index, config) {
	Base_Worker.call(this, 'write', index, config);
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
 * @param {WriterMessage} message
 */
WriterWorker.prototype.processMessage = function (message) {
	var self = this;
	var deltas = [];
	var remainingDeltas = false;

	async.series([
		function getDeltas(callback) {
			self.getAndRemoveDeltas(message.key, function (err, results, remaining) {
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
			self.processDeltas(deltas, function (err, results) {
				if (err) return callback(err);

				deltas = results;
				callback();
			});
		},
		function notifyTransportManager(callback) {
			tlib.services.messagingClient.send([JSON.stringify({ deltas: deltas })], 'transport_manager', function (err) {
				if (err) {
					tlib.services.logger.error('Failed to send message to transport_manager: ' + err.message);
				}
				callback();
			});
		}
	], function (err) {
		if (err)
			tlib.services.logger.error(err.toString());

		if (remainingDeltas)
			tlib.services.messagingClient.send([JSON.stringify({ key: message.key })], 'write', function (err) {
				if (err) {
					tlib.services.logger.error('Failed to send message to writers: ' + err.message);
				} else {
					tlib.services.logger.debug('Sent additional message to writers to grab deltas (fetchCount at ' + self.fetchCount + ')');
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
 * @param {string[]} redisKey The redis key where the deltas are stored
 * @param {getAndRemoveDeltasCb} callback
 */
WriterWorker.prototype.getAndRemoveDeltas = function (redisKey, callback) {
	var deltas = [];
	var transaction = tlib.services.redisClient.multi();

	transaction.zrange([redisKey, 0, this.fetchCount - 1]);
	transaction.zremrangebyrank([redisKey, 0, this.fetchCount - 1]);
	transaction.zcard([redisKey]);

	transaction.exec(function (err, replies) {
		if (err)
			return callback(new Error('Failed to get deltas: ' + err.message));

		async.each(replies[0], function (r, c) {
			deltas.push(JSON.parse(r));
			c();
		}, function () {
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
WriterWorker.prototype.processDeltas = function (deltas, callback) {
	
	var createDeltas = [];
	var updatePatches = [];
	var updateDeltaPatches = [];
	var deleteDeltaObjects = [];
	var deleteObjects = {};

	async.each(deltas, function (delta, c) {
		switch (delta.op) {
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
					tlib.users.delete(delta.object.id, delta.object.application_id, function () { });
				} else
					deleteObjects[delta.object.id] = delta.object.type;

				deleteDeltaObjects.push(delta);

				break;
		}
		c();
	}, function () {
		if (createDeltas.length) {
			let createModels=[];

			for (let i = 0; i < createDeltas.length; i++) {
				if (createDeltas[i].object.type === 'user') {
					tlib.users.new(createDeltas[i].object, () => {}); 
				} else {
					createModels.push(createDeltas[i]);
				}
			}
			if(createModels) {
				tlib.models.new(createModels, function (err) {
					if (err) tlib.services.logger.error('Error processing create requests: ' + err.message + '\n' + err.stack);
				}, function (createdItems) {
					callback(null, createdItems.concat(updateDeltaPatches, deleteDeltaObjects));
				});
			}
		}
		
		if (updatePatches.length) {
			let updateSeparator = { 
				user:[], 
				model: []
			};
		
			for (var i = 0; i < updatePatches.length; i++) {
				if (updatePatches[i].path.split('/')[0] === 'user') {
					updateSeparator.user.push(updatePatches[i]);
				} else {
					updateSeparator.model.push(updatePatches[i]);
				}
			}

			if (updateSeparator.model.length) {
				tlib.models.update(updateSeparator.model, function (err) {
					if (err) tlib.services.logger.error('Error processing update requests: ' + err.message);
				});
			}

			if (updateSeparator.user.length) {
				tlib.users.update(updateSeparator.user, function (err) {
					if (err) tlib.services.logger.error('Error processing update requests: ' + err.message);

				});
			}
		}

		if (Object.keys(deleteObjects).length) {
			tlib.models.delete(deleteObjects, function (err) {
				if (err) tlib.services.logger.error('Error processing update requests: ' + err.message);
			});
		}

		if (!createDeltas.length){
			callback(null, [].concat(updateDeltaPatches, deleteDeltaObjects));
		}
	});
};

module.exports = WriterWorker;
