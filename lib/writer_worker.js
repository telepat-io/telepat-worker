var Base_Worker = require('./base_worker');
var Models = require('telepat-models');
var async = require('async');
var cloneObject = require('clone');

var WriterWorker = function(index) {
	Base_Worker.call(this, 'write', index);
	this.messageCount = {};
};

WriterWorker.prototype = Object.create(Base_Worker.prototype);

WriterWorker.prototype.processMessage = function(message) {
	var self = this;
	var deltas = {};
	var contextDeltas = {};
	var subscriptions = message.keys;
	var contextSubscriptions = [];
	//var subscribedDevices = [];
	//var subscribedDeviceIds = [];
	var appId = message.applicationId;
	var profilingContext = new Models.ProfilingContext();

	profilingContext.initial();
	async.series([
		function(callback) {
			Models.Application(appId, function(err, app) {
				if (err) {
					err.message = 'Error in retrieving application with ID '+appId+': '+err.message;
					return callback(err);
				}
				Models.Application.loadedAppModels[app.id] = app;
				profilingContext.addMark('get-application');
				callback();
			});
		},
		function getContextDeltas(callback) {
			async.filter(subscriptions, function(subscription, c) {
				if((/^blg:(.)+:context$/g).test(subscription)) {
					contextSubscriptions.push(subscription);
					c(false);
				} else
					c(true);
			}, function(results) {
				subscriptions = results;
				profilingContext.addMark('get-context-deltas');
				callback();
			});
		},
		function getAndRemoveDeltas(callback) {
			if (subscriptions.length > 0) {
				Base_Worker.multiGetAndRemoveDeltas(subscriptions, function(err, results) {
					if (err) {
						err.messsage = 'Error in retrieving model deltas: '+err.message;
						return callback(err);
					}
					deltas = results;
					profilingContext.addMark('multiget-and-remove-deltas');
					callback();
				});
			} else {
				callback();
			}
		},
		function getAndRemoveContextDeltas(callback) {
			if (contextSubscriptions.length > 0) {
				Base_Worker.multiGetAndRemoveDeltas(contextSubscriptions, function(err, results) {
					if (err) {
						err.messsage = 'Error in retrieving context deltas: '+err.message;
						return callback(err);
					}
					contextDeltas = results;
					callback();
				});
			} else {
				callback();
			}
		},
		/*function(callback) {
			async.each(subscriptions, function(subscription, c) {
				self.getDeviceIdsFromSubscription(subscription, function(err, results) {
					if (err) {
						err.messsage = 'getDeviceIdsFromSubscription error for subscription '+subscription+': '
							+err.message;
						return c(err);
					}

					results.forEach(function(device) {
						subscribedDeviceIds.push(device);
					});
					c();
				});
			}, callback);
		},
		function(callback) {
			self.getDevices(subscribedDeviceIds, function(err, results) {
				if (err) {
					err.message = 'Error getting devices: '+err.message;
					return callback(err);
				}

				async.each(results, function(deviceObject, c) {
					subscribedDevices.push(deviceObject);
					c();
				}, callback);
			});
		},*/
		function(callback) {
			self.processDeltas(deltas, function(err, results) {
				deltas = results;
				profilingContext.addMark('process-deltas');
				callback();
			});
		},
		//process conextDeltas
		function(callback) {
			self.processDeltas(contextDeltas, function(err, results) {
				contextDeltas = results;
				callback();
			});
		},
		function(callback) {
			if (deltas.new.length)
				self.createItems(deltas.new, function(err) {
					if (err) return callback(err);
					profilingContext.addMark('create-items');
					callback()
				});
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
			self.notifyClientTransports(deltas, subscriptions, message.applicationId, callback);
		},
		function(callback) {
			profilingContext.addMark('notify-client-transports');
			if (contextSubscriptions.length > 0) {
				self.notifyClientTransports(contextDeltas, [], message.application_id, callback);
			} else
				callback();
		}
	], function(err) {
		if (err)
			Models.Application.logger.error(err.toString());

		var currentMinute = (new Date()).getMinutes();

		if (Object.keys(self.messageCount).length == 0) {
			self.messageCount[currentMinute] = 1;
		} else if (self.messageCount[currentMinute]) {
			self.messageCount[currentMinute]++;
		} else {
			var lastMinute = currentMinute == 0 ? 59 : currentMinute - 1;
			console.log('==================== Minute '+currentMinute+':\t'+self.messageCount[lastMinute] || 0);
			self.messageCount[currentMinute] = 1;
		}

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

	var processUpdate = function(op, oldValue, value) {
		switch(op) {
			case "increment": {
				return oldValue + value;
			}
			case "replace": {
				return value;
			}
			case "append": {
				if (typeof oldValue == 'string')
					return oldValue+d.value;
				else if (oldValue instanceof Array) {
					oldValue.push(value);
					return oldValue;
				} else
					return oldValue;
			}
			case "remove": {
				if (oldValue instanceof Array) {
					var idx = oldValue.indexOf(value);
					if (idx !== -1)
						oldValue.splice(idx, 1);
				}

				return oldValue;
			}
		}
	};

	/**
	 * @param {Delta} d
	 */
	async.each(deltas, function(d, callback1) {
		switch (d.op) {
			case "add": {
				newItems.push(Models.Delta.fromObject(d));  //{value: d.value, subscription: d.subscription, guid: d.guid});
				callback1();

				break;
			}
			case "increment" :
			case "replace":
			case "append":
			case "remove": {
				async.detectSeries(modifiedItemsPatches, function(modP, detectCallback) {
					return detectCallback(modP.path == d.path);
				}, function(res) {
					if(res && (res.subscription == d.subscription)) {
						res.value = processUpdate(d.op, res.value, d.value);
					} else
						modifiedItemsPatches.push(Models.Delta.fromObject(d));//{op: d.op, path: d.path, value: d.value, subscription: d.subscription, guid: d.guid});

					if (d.type == 'user')
						res.username = d.username;
					callback1();
				});

				break;
			}
			case "delete": {
				deletedItems.push(Models.Delta.fromObject(d));
				callback1();
				break
			}
		}
	}, function() {
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
				deltas[it-1].value.application_id = deltas[it - 1].application_id;
				Models.User.create(deltas[it-1].value, deltas[it-1].value.application_id, function(err) {
					if (err){
						Models.Application.logger.warning('Could not create user on application "'+
							deltas[it - 1].application_id+'": '+err.message);
					}

					operationIds[deltas[it-1].guid] = true;
					callback1();
				});
			} else if (deltas[it-1].value.type == 'context') {
				if (deltas[it-1].instant)
					callback1();
				else {
					Models.Context.create(deltas[it-1].value, function(err, result) {
						if (err) {
							Models.Application.logger.warning('Could not create context on application "'
								+ deltas[it - 1].application_id+'": '+err.message);
						}

						deltas.forEach(function(item, index, originalArray) {
							if (item.guid == deltas[it-1].guid) {
								originalArray[index].value = result;
							}
						});

						operationIds[deltas[it-1].guid] = true;
						callback1();
					});
				}
			} else {
				Models.Model.create(deltas[it - 1].value.type, deltas[it - 1].application_id, deltas[it - 1].value, function (err1, result1) {
					if (err1) {
						Models.Application.logger.warning('Could not create model "'+deltas[it - 1].value.type
							+'" on application "'+ deltas[it - 1].application_id+'": '+err1.message);
					}

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
						//modItem.path = pathParts[2];
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
					if (objectPatches[objectPath][0].instant) {
						c();
					} else {
						var username = objectPatches[objectPath][0].username;
						var userApp = objectPatches[objectPath][0].application_id;
						Models.User.update(username, userApp, objectPatches[objectPath], function(err) {
							if (err) {
								Models.Application.logger.warning('Could not update user "'+username+'" on application "'+
								userApp+'": '+err.message);
							}
							c();
						});
					}
				} else if (pathParts[0] == 'context') {
					if (objectPatches[objectPath][0].instant)
						c();
					else {
						var contextId = pathParts[1];
						var contextApp = objectPatches[objectPath][0].application_id;
						Models.Context.update(contextId, objectPatches[objectPath], function(err) {
							if (err) {
								Models.Application.logger.warning('Could not update context "'+contextId+
									'" on application "'+contextApp+'": '+err.message);
							}
							c();
						});
					}
				} else {
					var objectId = pathParts[0];
					var objectContext = objectPatches[objectPath][0].context;
					var objectApp = objectPatches[objectPath][0].application_id;
					var objectModel = pathParts[1];
					Models.Model.update(objectId, objectContext, objectApp, objectModel, objectPatches[objectPath], function(err) {
						if (err) {
							Models.Application.logger.warning('Could not update model "'+objectModel+
								'" wid ith "'+objectId+'" on application "'+objectApp+'": '+err.message);
						}

						c();
					});
				}

			}, callback1);
		}
	], callback);
};

WriterWorker.prototype.deleteItems = function(deltas, callback) {
	var operationIds = {};

	async.each(deltas, function(delItem, callback1){
		if (operationIds[delItem.guid])
			return callback1();

		operationIds[delItem.guid] = true;

		var pathParts = delItem.path.split('/');//model/id

		if (pathParts[0] == 'user') {
			if (delItem.instant) {
				callback1();
			} else {
				Models.User.delete(delItem.username, delItem.application_id, function(err) {
					if (err) {
						Models.Application.logger.warning('Could not delete user "'+delItem.username+'" on application "'+
						delItem.application_id+'": '+err.message);
					}
					callback1();
				});
			}
		} else if (pathParts[0] == 'context')  {
			if (delItem.instant)
				callback1();
			else {
				Models.Context.delete(pathParts[1], function(err) {
					if (err) {
						Models.Application.logger.warning('Could not delete context "'+pathParts[1]+'": '+err.message);
					}

					callback1();
				});
			}
		} else {
			Models.Model.delete(pathParts[0], delItem.application_id, delItem.context, pathParts[1], false, function(err) {
				if (err) {
					Models.Application.logger.warning('Could not delete model "'+pathParts[0]+
					'" with ID "'+pathParts[1]+'" on application "'+delItem.application_id+'": '+err.message);
				}
				callback1();
			});
		}
	}, callback);
};

WriterWorker.prototype.notifyClientTransports = function(deltas, subscriptions, appId, callback) {
	//var self = this;
	this.messagingClient.publish([JSON.stringify({subscriptions: subscriptions, deltas: deltas, appId: appId})], callback);
	/*async.each(subscribedDevices, function(device, c) {
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
			Models.Application.logger.warning('Skipping device with ID "'+device.id+'": no volatile or persistent' +
				'notification config present.');
			return c();
		}

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

			if (transportType == 'sockets') {
				if (!device.volatile.server_name) {
					Models.Application.logger.error('Cannot send message to socket servers, server_name is' +
						'missing from the device info');
				} else {
					self.messagingClient.send([JSON.stringify(transportMessage)], device.volatile.server_name, function(err) {
						if (err) {
							Models.Application.logger.error('Failed to send message to '+device.volatile.server_name+': '+err.message);
						}
					});
				}
			} else {
				var topicName = transportType+'_transport';

				self.messagingClient.send([JSON.stringify(transportMessage)], topicName, function(err) {
					if (err) {
						Models.Application.logger.error('Failed to send message to '+topicName+': '+err.message);
					}
				});
			}
		});

		c();
	}, callback);*/
};

WriterWorker.prototype.sendClientNotificationsForContext = function(deltas, callback) {
	var subscribedDevices = {};
	var applicationDeltas = {};

	var self = this;

	var functionGetDevices = function(delta, c) {
		var applicationId = delta.subscription.split(':')[1]; // blg:{appId}:context

		var operation = null;

		if (delta.op == Models.Delta.OP.ADD)
			operation = 'new';
		else if (delta.op == Models.Delta.OP.DELETE)
			operation = 'deleted';
		else
			operation = 'updated';

		if (!applicationDeltas[applicationId])
			applicationDeltas[applicationId] = {new: [], updated: [], deleted: []};
		applicationDeltas[applicationId][operation].push(delta);

		if (subscribedDevices[applicationId])
			return c();

		subscribedDevices[applicationId] = {};

		Models.Subscription.getAllDevices(applicationId, function(err, devices) {
			if (err) return c(err);
			subscribedDevices[applicationId] = devices;
			c();
		});
	};

	async.series([
		function(callback1) {
			async.parallel([
				//we must check all deltas since they may come from different applications
				function(callback2) {
					async.each(deltas.new, functionGetDevices, callback2);
				},
				function(callback2) {
					async.each(deltas.updated, functionGetDevices, callback2);
				},
				function(callback2) {
					async.each(deltas.deleted, functionGetDevices, callback2);
				}
			], callback1);
		},
		function(callback1) {
			async.each(Object.keys(subscribedDevices), function(applicationId, c) {
				var transportType = null;
				var devices = subscribedDevices[applicationId];

				async.each(devices, function(device, c2) {
					var transportMessage = {device: null, deltas: applicationDeltas[applicationId], applicationId: applicationId};
					transportMessage.device = device;

					if (device.volatile && device.volatile.active == 1) {
						transportType = device.volatile.type;
						//console.log(device.id+': [volatile] - '+device.volatile.token);
					} else if(device.persistent) {
						transportType = device.persistent.type;
						//console.log(device.id+': [persistent] - '+device.persistent.token);
					} else	{
						Models.Application.logger.warning('Skipping device with ID "'+device.id+
						'": no volatile or persistent notification config present.');
						return c2();
					}

					if (transportType == 'sockets') {
						if (!device.volatile.server_name) {
							Models.Application.logger.error('Cannot send message to socket servers, server_name is' +
								'missing from the device info');
						} else {
							self.messagingClient.send([JSON.stringify(transportMessage)], device.volatile.server_name, function(err) {
								if (err) {
									Models.Application.logger.error('Failed to send message to '+device.volatile.server_name+': '+err.message);
								}
							});
						}
					} else {
						var topicName = transportType+'_transport';

						self.messagingClient.send([JSON.stringify(transportMessage)], topicName, function(err) {
							if (err) {
								Models.Application.logger.error('Failed to send message to '+topicName+': '+err.message);
							}
						});
					}
					c2();
				}, c);
			}, callback1);
		}
	], callback);
};

module.exports = WriterWorker;
