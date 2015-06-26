var async = require('async');
var Models = require('octopus-models-api');
var cb = require('couchbase');
var mandrill = require('mandrill-api/mandrill');
var mandrillClient = new mandrill.Mandrill('NT-lxYd5VPu7CyyW2sduVg');
var bfj = require('bfj');
var stream = require('stream');
var cloneObject = require('clone');
var microtime = require('microtime-nodejs');

/**
 * The writer receives the subscription key that has changes alongside with the operation identifier. It forms the deltas key from this key and processes the
 * deltas in the document. Each operation is only carried out
 */
function Write(message) {
    var keys = [];

	message.keys.forEach(function(val) {
		if (val !== null)
			keys.push(val);
	});

	if (!keys.length)
		return null;

	/**
	 *
	 * @type {{
	 * device_id: {
	 * 		object: {},
	 * 		patches: {
	 * 			new: [],
	 * 			updated: [],
	 *			deleted: []
	 * 		}
	 * 	}
	 * }}
	 */
    var subscribedDevices = {};
	/**
	 *
	 * @type {{
	 * 		subscription_id: [devices]
	 * }}
	 */
	var subscriptionDevices = {};
	var operationIds = {};

    async.waterfall([
        function getDevicesArray(callback) {
			async.each(keys, function(key, c) {
				var keyParts = key.split(':');
				keyParts.pop();
				key = keyParts.join(':');

				stateBucket.get(key, function(err, result) {
					if (err && err.code == cb.errors.keyNotFound) {
						console.log(('Subscription key "'+key+'"').red+' not found');
						return c();
					}
					else if (err)
						return c(err);

					if (!result.value)
						return c();
					else {
						var stringStream = new stream.Readable();
						stringStream._read = function noop(){};
						stringStream.push('['+result.value.slice(0, -1)+']');
						stringStream.push(null);
						bfj.parse(stringStream).then(function(data) {
							data.forEach(function(item) {
								if (!subscriptionDevices[key])
									subscriptionDevices[key] = [];
								subscriptionDevices[key].push(item);
								subscribedDevices[item] = {
									object: {},
									patches: {
										'new': [],
										'updated': [],
										'deleted': []
									}
								};
							});
							c();
						});
					}
				});
			}, callback);
        },
		function getDeviceObjects(callback) {
			Models.Subscription.multiGetDevices(Object.keys(subscribedDevices), function(err, results) {
				if (err) console.log('Failed'.red+' to get '+err+' device keys.');
				async.each(Object.keys(results), function(d, c) {
					subscribedDevices[d].object = results[d];
					c();
				}, callback);
			});
		},
		function getAndRemoveDeltas(callback) {
			var deltas = [];

			var transaction = redisClient.multi();

			keys.forEach(function(item) {
				transaction.lrange([item, 0, -1]);
			});

			transaction.del(keys).exec(function(err, replies) {
				if (err)
					return callback(new Error('failed to execute transaction'));

				keys.forEach(function(subscriptionKey, index) {
					//injectin sub key into patches
					var parsedResponse = JSON.parse('['+replies[index]+']');
					var transportMessageTimestamp = microtime.now();
					var lastPatchOperation = null;

					parsedResponse.forEach(function(patchValue, patchIndex, patchesArray) {
						patchesArray[patchIndex].subscription = subscriptionKey.replace(':deltas', '');

						if (['replace', 'increment', 'append'].indexOf(patchesArray[patchIndex].op) !== -1) {
							var pathParts = patchValue.path.split('/'); //modelname/id/fieldname
							var transportMessageKey = 'blg:'+message.applicationId+':'+pathParts.join(':')+':transport_msg_timestamp';
							patchesArray[patchIndex]._microtime = transportMessageTimestamp;

							//we don't want to unnecessary writes to the same key with the same value
							//because the same operation can be in multiple subscriptions
							if (lastPatchOperation !== patchValue.guid) {
								redisClient.set(transportMessageKey, transportMessageTimestamp, function(err, result) {
									redisClient.expire(transportMessageKey, 60);
								});
							}
							lastPatchOperation = patchValue.guid;
						}
					});

					deltas = deltas.concat(parsedResponse);
				});

				async.sortBy(deltas, function(item, c) {
					c(null, item.ts);
				},
				function(err, results) {
					deltas = results;
					callback(null, deltas);
				});
			});
		},
        function processDeltas(result, callback) {
            var deltas = result;

            var newItems = [];
            var modifiedItemsPatches = [];
            var deletedItems = [];

            async.each(deltas, function(d, callback1) {
				switch (d.op) {
                    case "add": {
						newItems.push({value: d.value, subscription: d.subscription, guid: d.guid});

                        break;
                    }
                    case "increment" : {
						async.detectSeries(modifiedItemsPatches, function(modP, detectCallback) {
							return detectCallback(modP.path == d.path);
						}, function(res) {
							if(res && (res.subscription == d.subscription)) {
								res.value += d.value;
							} else
								modifiedItemsPatches.push({op: d.op, path: d.path, value: d.value, subscription: d.subscription, guid: d.guid, _microtime: d._microtime});

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
								modifiedItemsPatches.push({op: d.op, path: d.path, value: d.value, subscription: d.subscription, guid: d.guid, _microtime: d._microtime});

							if (d.type == 'user')
								res.email = d.email;
						});

                        break;
                    }
                    case "delete": {
						deletedItems.push(d);
                        break
                    }
                }
				callback1();
            }, function(err) {
				callback(null, newItems, modifiedItemsPatches, deletedItems);
            });
        },
        function updateDatabase(newItems, modifiedItemsPatches, deletedItems, callback) {
			var it = 0;

            async.series([
				function CreateItems(callback1) {
					async.whilst(function() { return it < newItems.length; },
                        function(callback2) {
							it++;

							if (operationIds[newItems[it-1].guid])
								return callback2();

							if (newItems[it-1].value.type == 'user') {
								Models.User.create(newItems[it-1].value, callback2);
							} else if (newItems[it-1].value.type == 'context') {
								Models.Context.create(newItems[it-1].value, callback2);
							} else {
								Models.Model.create(newItems[it - 1].value.type, message.applicationId, newItems[it - 1].value, function (err1, result1) {
									if (err1) return callback2(err1);

									operationIds[newItems[it-1].guid] = true;
									callback2();
								});
							}
                        },
                        callback1);
                },
                function UpdateItems(callback1) {
					async.each(modifiedItemsPatches, function(item, callback2) {
                        var pathParts = item.path.split('/'); //model/id/fieldname

						if (operationIds[item.guid])
							return callback2();

						operationIds[item.guid] = true;

						async.detectSeries(deletedItems, function(delItem, c) {
							return c(delItem.path == item.path);
						}, function(result) {
							if (result)	{
								delete modifiedItemsPatches[item];
								callback2();
							} else {
								if (message.user) {
									Models.User.update(item.email, [item], callback2);
								} else if (message.context) {
									Models.Context.update(pathParts[0], [item], callback2);
								} else {
									var modItem = cloneObject(item);
									modItem.path = pathParts[2];
									Models.Model.update(pathParts[0], message.applicationId, pathParts[1], [modItem], callback2);
								}
							}
						});
                    }, callback1);
                },
                function DeleteItems(callback1) {
                    async.each(deletedItems, function(delItem, callback2){

						if (operationIds[delItem.guid])
							return callback2();

						operationIds[delItem.guid] = true;

						var pathParts = delItem.path.split('/');//model/id

						if (message.user) {
							Models.User.delete(delItem.email, function(err, result) {
								if (err) return console.log(err);

								deleteUserRelatedKeys(result, callback2);
							});
						} else if (message.context)  {
							Models.Context.delete(pathParts[1], callback2);
						} else {
							Models.Model.delete(pathParts[0], message.applicationId, pathParts[1], false, callback2);
						}
                    }, callback1);
                },
				function addPatchesToDevices(callback1) {
					var allItems = {'new': newItems, 'updated': modifiedItemsPatches, 'deleted': deletedItems};
					async.each(Object.keys(allItems), function (op, c) {
						async.each(allItems[op], function(p, c2) {
							var devicesForSub = subscriptionDevices[p.subscription];
							async.each(devicesForSub, function(dev, c3) {
								subscribedDevices[dev].patches[op].push(p);
								c3();
							}, c2);
						}, c);
					}, callback1);
				}
            ], function(err) {
                if (err) return callback(err);

				async.series([
					function(callback1) {
						if (message.context) {
							Models.Subscription.getAllDevices(function(err, results) {
								subscribedDevices = results;
								callback1();
							});
						} else
							callback1();
					},
					function(callback1) {
						async.each(Object.keys(subscribedDevices), function (d, callback2) {
							if (subscribedDevices[d].object.transport_type == 'email') {
								var message = {
									text: JSON.stringify(clientPatches),
									subject: "Notification",
									from_email: "razvan@appscend.com",
									from_name: "Razvan Botea",
									to: [{
										email: subscribedDevices[d].object.email,
										type: "to"
									}]
								};

								mandrillClient.messages.send({message: message, async: true}, function (result) {
									console.log(result);
								}, function (e) {
									console.log('A mandrill error occurred: ' + e.name + ' - ' + e.message);
								});
							}
							callback2();
						}, function(err) {
							async.each(subscribedDevices, function(dev, c) {
								var transportType = null;

								if (!Object.getOwnPropertyNames(dev.object).length)
									return c();

								if (dev.object.volatile && dev.object.volatile.active) {
									transportType = dev.object.volatile.type;
								} else {
									transportType = dev.object.persistent.type;
								}

								var topicName = transportType+'_transport';

								dev.applicationId = message.applicationId;

								kafkaProducer.send([{
									topic: topicName,
									messages: [JSON.stringify(dev)]
								}], function(err) {
									if (err) console.log(('Failed kafka message to '+topicName).red+' '+err);
								});

								c();
							}, callback1);
						});
					}
				], callback);

				callback();
            }) ;
        }
    ], function(err, results) {
        if (err) {
            console.log(err);
            console.trace('Writer stack');
        }
    });
}

function deleteUserRelatedKeys(user, callback) {
	async.waterfall([
		function deleteSubscriptions(result, callback1) {
			async.each(Object.keys(user.subscriptions), function(deviceId, c1) {
				async.each(user.subscriptions[deviceId], function(sub, c2) {

					async.waterfall([
						function(callback2) {
							var deviceKey = 'blg:devices:'+deviceId;

							Models.Application.stateBucket.remove(deviceKey, callback2);
						},
						function(callback2) {
							Models.Application.stateBucket.getAndLock(sub, callback2);
						},
						function(result, callback2) {
							var devices = JSON.parse('['+result.value.slice(0, -1)+']');
							var idx = devices.indexOf(deviceId);

							if (idx !== -1)
								devices.splice(idx, 1);

							async.map(devices, function(item, c) {
								c('"'+item+'"');
							}, function(resultDevices) {
								Models.Application.stateBucket.replace(sub, resultDevices.toString()+',', {cas: result.cas}, callback2);
							});
						}
					], c2);

				}, c1);
			}, callback1);
		},
		function deleteItems(result, callback1) {
			var query = cb.ViewQuery.from('models', 'by_author').custom({inclusive_end: true, key: user.id});

			async.waterfall([
				function(callback2) {
					Models.Application.bucket.query(query, callback2);
				},
				function(results, callback2) {
					async.each(Object.keys(results), function(objectKey, c) {
						Models.Application.bucket.remove(objectKey, c);
					}, callback2);
				}
			], callback1);
		},
		function deleteLookupKeys(result, callback1) {
			var query = cb.ViewQuery.from('models', 'by_author_lookup').custom({inclusive_end: true, key: user.id});
			async.waterfall([
				function(callback2) {
					Models.Application.bucket.query(query, callback2);
				},
				function(callback2) {
					async.each(result, function(item, c) {
						Models.Application.bucket.remove(item.id, c);
					}, callback2);
				}
			], callback1);
		},
		function deleteUserInFriends(result, callback1) {
			async.each(user.friends, function(f, c) {
				var key = 'blg:'+User._model.namespace+':fid:'+f;
				async.waterfall([
					function(callback2) {
						Models.Application.bucket.get(key, callback2);
					},
					function(result, callback2) {
						var userEmail = result.value;
						new User(userEmail, callback2);
					},
					function(result, callback2) {
						var idx = result.friends.indexOf(f);
						if (idx !== -1)
							result.friends.splice(idx, 1);

						Models.Application.bucket.replace(key, result, callback2);
					}
				], c);
			}, callback1);
		}
	], callback);
}

module.exports = Write;
