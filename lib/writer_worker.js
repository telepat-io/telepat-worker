var Base_Worker = require('./base_worker');
var Models = require('telepat-models');
var async = require('async');
var cloneObject = require('clone');

var WriterWorker = function(index) {
	Base_Worker.call(this, 'write', index);
};

WriterWorker.prototype = new Base_Worker();

WriterWorker.prototype.processMessage = function(message) {
	var self = this;
	var deltas = {};
	var subscriptions = message.keys;
	var subscribedDevices = {};

	var profiler = new Models.ProfilingContext();

	async.series([
		function getAndRemoveDeltas(callback) {
			profiler.initial();
			Base_Worker.multiGetAndRemoveDeltas(subscriptions, message.applicationId, function(err, results) {
				if (err) return callback(err);
				deltas = results;
				callback();
			});
		},
		function(callback) {
			profiler.addMark('getAndRemoveDeltas');
			async.each(subscriptions, function(subscription, c) {
				self.getDeviceIdsFromSubscription(subscription, function(err, results) {
					if (err) return c(err);

					results.forEach(function(deviceId) {
						subscribedDevices[deviceId] = {};
					});
					c();
				});
			}, callback);
		},
		function(callback) {
			profiler.addMark('getDeviceIdsFromSubscriptions');
			self.getDevices(Object.keys(subscribedDevices), function(err, results) {
				if (err) return callback(err);

				async.each(results, function(deviceObject, c) {
					subscribedDevices[deviceObject.id] = deviceObject;
					c();
				}, callback);
			});
		},
		function(callback) {
			profiler.addMark('getDevices');
			self.processDeltas(deltas, function(err, results) {
				deltas = results;
				callback();
			});
		},
		function(callback) {
			profiler.addMark('processDeltas');
			if (deltas.new.length)
				self.createItems(deltas.new, callback);
			else
				callback();
		},
		function(callback) {
			if (deltas.updated.length)
				self.updateItems(deltas.updated, deltas.deleted, callback);
			else
				callback();
		},
		function(callback) {
			if (deltas.deleted.length)
				self.deleteItems(deltas.deleted, callback);
			else
				callback();
		},
		function(callback) {
			profiler.addMark('new+update+deleted');
			self.sendClientNotifications(deltas, subscribedDevices, message.applicationId, callback);
		}
	], function(err) {
		if (err)
			console.log(err);
	})
};

/**
 *
 * @param deltas
 * @param callback
 */
WriterWorker.prototype.processDeltas = function(deltas, callback) {
	var newItems = [];
	var modifiedItemsPatches = [];
	var deletedItems = [];

	/**
	 * @param {Delta} d
	 */
	async.each(deltas, function(d, callback1) {
		switch (d.op) {
			case "add": {
				newItems.push(Models.Delta.fromObject(d));  //{value: d.value, subscription: d.subscription, guid: d.guid});

				break;
			}
			case "increment" : {
				async.detectSeries(modifiedItemsPatches, function(modP, detectCallback) {
					return detectCallback(modP.path == d.path);
				}, function(res) {
					if(res && (res.subscription == d.subscription)) {
						res.value += d.value;
					} else
						modifiedItemsPatches.push(Models.Delta.fromObject(d));//{op: d.op, path: d.path, value: d.value, subscription: d.subscription, guid: d.guid});

					if (d.type == 'user')
						res.email = d.email;
				});

				break;
			}
			case "replace": {
				async.detectSeries(modifiedItemsPatches, function(modP, detectCallback) {
					return detectCallback(modP.path == d.path);
				}, function(res) {
					if(res && (res.subscription == d.subscription)) {
						res.value = d.value;
					} else
						modifiedItemsPatches.push(Models.Delta.fromObject(d));//{op: d.op, path: d.path, value: d.value, subscription: d.subscription, guid: d.guid});

					if (d.type == 'user')
						res.email = d.email;
				});

				break;
			}
			case "delete": {
				deletedItems.push(Models.Delta.fromObject(d));
				break
			}
		}
		callback1();
	}, function(err) {
		callback(null, {new: newItems, updated: modifiedItemsPatches, deleted: deletedItems});
	});
};

WriterWorker.prototype.createItems = function(deltas, callback) {
	var it = 0;
	var operationIds = {};

	async.whilst(function() { return it < deltas.length; },
		function(callback1) {
			it++;

			if (operationIds[deltas[it-1].guid])
				return callback1();

			if (deltas[it-1].value.type == 'user') {
				deltas[it-1].value.application_id = parseInt(deltas[it - 1].application_id);
				Models.User.create(deltas[it-1].value, callback1);
			} else if (deltas[it-1].value.type == 'context') {
				Models.Context.create(deltas[it-1].value, callback1);
			} else {
				Models.Model.create(deltas[it - 1].value.type, deltas[it - 1].application_id, deltas[it - 1].value, function (err1, result1) {
					if (err1) return callback1(err1);

					deltas.forEach(function(item, index, originalArray) {
						if (item.guid == deltas[it-1].guid) {
							originalArray[index].value = result1;
						}
					});

					operationIds[deltas[it-1].guid] = true;
					callback1();
				});
			}
		},
		callback);
};

WriterWorker.prototype.updateItems = function(deltas, deletedItemsDeltas, callback) {
	var operationIds = {};
	var objectPatches = {};

	async.series([
		function(callback1) {
			async.each(deltas, function(item, callback2) {
				var pathParts = item.path.split('/'); //model/id/fieldname

				if (operationIds[item.guid])
					return callback2();

				operationIds[item.guid] = true;

				async.detectSeries(deletedItemsDeltas, function(delItem, c) {
					return c(delItem.path == item.path);
				}, function(result) {
					if (result)	{
						delete deltas[item];
						callback2();
					} else {
						var modItem = cloneObject(item);
						modItem.path = pathParts[2];
						if (!objectPatches[pathParts[0]+'/'+pathParts[1]]) {
							objectPatches[pathParts[0]+'/'+pathParts[1]] = [modItem];
						} else {
							objectPatches[pathParts[0]+'/'+pathParts[1]].push(modItem);
						}
						callback2();
					}
				});
			}, callback1);
		},
		function(callback1) {
			async.each(Object.keys(objectPatches), function(objectPath, c) {
				var pathParts = objectPath.split('/');

				if (pathParts[0] == 'user') {
					Models.User.update(objectPatches[objectPath][0].email, objectPatches[objectPath], c);
				} else if (pathParts[0] == 'context') {
					Models.Context.update(pathParts[1], objectPatches[objectPath], c);
				} else {
					Models.Model.update(pathParts[0], objectPatches[objectPath][0].context, objectPatches[objectPath][0].application_id, pathParts[1], objectPatches[objectPath], c);
				}

			}, callback1);
		}
	], callback);
};

WriterWorker.prototype.deleteItems = function(deltas, callback) {
	var operationIds = {};
	var self = this;

	async.each(deltas, function(delItem, callback1){
		if (operationIds[delItem.guid])
			return callback1();

		operationIds[delItem.guid] = true;

		var pathParts = delItem.path.split('/');//model/id

		if (pathParts[0] == 'user') {
			Models.User.delete(delItem.email, function(err, result) {
				if (err) return console.log(err);

				self.deleteUserRelatedKeys(result, callback1);
			});
		} else if (pathParts[0] == 'context')  {
			Models.Context.delete(pathParts[1], callback1);
		} else {
			Models.Model.delete(pathParts[0], delItem.context, delItem.application_id, pathParts[1], false, callback1);
		}
	}, callback);
};

WriterWorker.prototype.getDeviceIdsFromSubscription = function(channel, callback) {
	Models.Application.redisClient.smembers(channel, callback);
};

WriterWorker.prototype.getDevices = function(deviceIds, callback) {
	if (!deviceIds.length)
		return callback(null, {});

	deviceIds = deviceIds.map(function(id) {
		return 'blg:devices:'+id;
	});

	Models.Application.redisClient.mget(deviceIds, function(err, results) {
		if (err) return callback(err);

		var deviceObjects = {};

		async.map(results, function(device, c) {
			if (device) {
				var parsedDevice = JSON.parse(device);
				deviceObjects[parsedDevice.id] = parsedDevice;
			}
			c();
		}, function() {
			callback(null, deviceObjects);
		});
	});
};

WriterWorker.prototype.sendClientNotifications = function(deltas, subscribedDevices, appId, callback) {
	var self = this;

	async.each(subscribedDevices, function(device, c) {
		var transportType = null;
		var transportMessage = {device: null, deltas: {new: [], updated: [], deleted: []}, applicationId: appId};
		transportMessage.device = device;

		if (device.volatile && device.volatile.active == 1) {
			transportType = device.volatile.type;
			//console.log(device.id+': [volatile] - '+device.volatile.token);
		} else if(device.persistent) {
			transportType = device.persistent.type;
			//console.log(device.id+': [persistent] - '+device.persistent.token);
		} else	{
			//console.log('Skipping device with ID: '+device.id);
			return c();
		}

		var topicName = transportType+'_transport';

		async.each(device.subscriptions, function(subscription, c1) {
			async.parallel([
				function(parallelCallback) {
					async.each(deltas.new, function(delta, c2) {
						if (delta.subscription == subscription)
							transportMessage.deltas.new.push(delta);
						c2();
					}, parallelCallback);
				},
				function(parallelCallback) {
					async.each(deltas.updated, function(delta, c2) {
						if (delta.subscription == subscription)
							transportMessage.deltas.updated.push(delta);
						c2();
					}, parallelCallback);
				},
				function(parallelCallback) {
					async.each(deltas.deleted, function(delta, c2) {
						if (delta.subscription == subscription)
							transportMessage.deltas.deleted.push(delta);
						c2();
					}, parallelCallback);
				}
			], c1);
		}, function() {
			//console.log('Sending kafka message to '+topicName+'for device '+device.id);
			self.messagingClient.send([{
				topic: topicName,
				messages: [JSON.stringify(transportMessage)]
			}], function(err) {
				if (err) console.log(('Failed kafka message to '+topicName).red+' '+err);
			});
		});

		c();
	}, callback);
};

module.exports = WriterWorker;
