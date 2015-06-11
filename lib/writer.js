var async = require('async');
var Models = require('octopus-models-api');
var cb = require('couchbase');
var mandrill = require('mandrill-api/mandrill');
var mandrillClient = new mandrill.Mandrill('NT-lxYd5VPu7CyyW2sduVg');
var bfj = require('bfj');
var stream = require('stream');

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

    var subscribedDevices = [];
	var operationIds = {};
	var clientPatches = {'new': [], updated: [], removed: []};

    //I: get & delete delta
    //II: aggregate deltas
    //III: delete unique IDs

    async.waterfall([
        function getDevicesArray(callback) {
			var devices = {};

			async.each(keys, function(key, c) {
				var keyParts = key.split(':');
				keyParts.pop();

				stateBucket.get(keyParts.join(':'), function(err, result) {
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
								devices[item] = true;
							});
							c();
						});
					}
				});
			}, function() {callback(null, devices);});
        },
		function getDeviceObjects(deviceIds, callback) {
			if (Object.keys(deviceIds).length) {
				Models.Subscription.multiGetDevices(Object.keys(deviceIds), function(err1, results) {
					if (err1) return callback(err1);

					subscribedDevices = results;
					callback();
				});
			} else
				callback();
		},
        function getDeltas(callback) {
            stateBucket.getMulti(keys, function(err, results) {
				if (err > 0) console.log('Failed'.red+' to get '+err+' subscription delta keys.');
				var deltas = [];

				async.each(Object.keys(results), function(subKey, c) {
					results[subKey].value.deltas.forEach(function(value, key, array) {
						array[key].subscription = subKey;
					});
					deltas = deltas.concat(results[subKey].value.deltas);
					c();
				}, function() {
					callback(null, deltas);
				});
			});
        },
		function removeDeltas(result, callback) {
			async.each(keys, function(k, c) {
				stateBucket.remove(k, function(err, results) {
					if (err) console.log(err);
				});
				c();
			});
			callback(null, result);
		},
        function processDeltas(result, callback) {
            var deltas = result;

            var newItems = [];
            var modifiedItemsPatches = {};
            var deletedItems = {};

            async.each(deltas, function(d, callback1) {
				switch (d.op) {
                    case "add": {
                        clientPatches.new.push({value: d.value});

						if (!operationIds[d.guid])
							newItems.push({value: d.value});

                        break;
                    }
                    case "increment" : {
						var pathParts = d.path.split('/');//model/id/field_name

						if (clientPatches.updated[d.path])
							clientPatches.updated[d.path].value += d.value;
						else
							clientPatches.updated[d.path] = {op: d.op, path: pathParts[2], value: d.value};

						if (d.type == 'user')
							clientPatches.updated[d.path].email = d.email;

						if (!operationIds[d.guid]) {
							if (modifiedItemsPatches[d.path])
								modifiedItemsPatches[d.path].value += d.value;
							else
								modifiedItemsPatches[d.path] = {op: d.op, path: pathParts[2], value: d.value};

							if (d.type == 'user')
								modifiedItemsPatches[d.path].email = d.email;
						}

                        break;
                    }
                    case "replace": {
                        var pathParts = d.path.split('/');//model/id/field_name

						clientPatches.updated[d.path] = {op: d.op, path: pathParts[2], value: d.value};

						if (d.type == 'user')
							clientPatches.updated[d.path].email = d.email;

						if (!operationIds[d.guid]) {
							modifiedItemsPatches[d.path] = {op: d.op, path: pathParts[2], value: d.value};

							if (d.type == 'user')
								modifiedItemsPatches[d.path].email = d.email;
						}

                        break;
                    }
                    case "delete": {
						clientPatches.removed = d;

						if (!operationIds[d.guid]) {
							deletedItems[d.path] = d;
							delete deletedItems[d.path].subscription;
						}

                        break
                    }
                }

				operationIds[d.guid] = true;
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
							if (newItems[it-1].value.type == 'user') {
								Models.User.create(newItems[it-1].value, callback2);
							} else if (newItems[it-1].value.type == 'context') {
								Models.Context.create(newItems[it-1].value, callback2);
							} else {
								Models.Model.create(newItems[it - 1].value.type, message.applicationId, newItems[it - 1].value, function (err1, result1) {
									if (err1) return callback2(err1);

									var id = Object.keys(result1)[0];
									var val = {};
									val[newItems[it - 1].value.type +'/'+ id] = result1;

									clientPatches.new.push({op: 'add', value: val});
									callback2();
								});
							}
                        },
                        callback1);
                },
                function UpdateItems(callback1) {
					async.each(Object.keys(modifiedItemsPatches), function(item, callback2) {
                        var pathParts = item.split('/'); //model/id/fieldname

                        if (deletedItems[pathParts[0]+pathParts[1]]) {
							delete modifiedItemsPatches[item];
							return callback2();
						}

						if (message.user) {
							Models.User.update(modifiedItemsPatches[item].email, [modifiedItemsPatches[item]], callback2);
						} else if (message.context) {
							Models.Context.update(pathParts[0], [modifiedItemsPatches[item]], callback2);
						} else {
							Models.Model.update(pathParts[0], message.applicationId, pathParts[1], [modifiedItemsPatches[item]], callback2);
						}
                    }, function(err) {
						clientPatches.updated = modifiedItemsPatches;
						callback1(err);
					});
                },
                function DeleteItems(callback1) {
					clientPatches.removed = deletedItems;

                    async.each(Object.keys(deletedItems), function(delPath, callback2){
                        var pathParts = delPath.split('/');//model/id

						if (message.user) {
							Models.User.delete(deletedItems[delPath].email, function(err, result) {
								if (err) return console.log(err);

								deleteUserRelatedKeys(result, callback2);
							});
						} else if (message.context)  {
							Models.Context.delete(pathParts[1], callback2);
						} else {
							Models.Model.delete(pathParts[0], message.applicationId, pathParts[1], callback2);
						}
                    }, callback1);
                }
            ], function(err) {
                if (err) return callback(err);

				var deviceTokens = {};

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
							if (subscribedDevices[d].transport_type == 'email') {
								var message = {
									text: JSON.stringify(clientPatches),
									subject: "Notification",
									from_email: "razvan@appscend.com",
									from_name: "Razvan Botea",
									to: [{
										email: subscribedDevices[d].email,
										type: "to"
									}]
								};

								mandrillClient.messages.send({message: message, async: true}, function (result) {
									console.log(result);
								}, function (e) {
									console.log('A mandrill error occurred: ' + e.name + ' - ' + e.message);
								});
							} else {
								if (subscribedDevices[d].volatile && subscribedDevices[d].volatile.active) {
									if (deviceTokens[subscribedDevices[d].volatile.type] === undefined)
										deviceTokens[subscribedDevices[d].volatile.type] = [subscribedDevices[d].volatile.token];
									else
										deviceTokens[subscribedDevices[d].volatile.type].push(subscribedDevices[d].volatile.token);

								} else {
									if (deviceTokens[subscribedDevices[d].persistent.type] === undefined)
										deviceTokens[subscribedDevices[d].persistent.type] = [[subscribedDevices[d].persistent.token]];
									else
										deviceTokens[subscribedDevices[d].persistent.type].push([subscribedDevices[d].persistent.token]);
								}
							 }
							callback2();
						}, function(err) {
							for(var transportType in deviceTokens) {
								var topicName = transportType+'_transport';

								kafkaProducer.send([{
									topic: topicName,
									messages: [JSON.stringify({devices: deviceTokens[transportType], data: {patches: clientPatches}})]
								}], function(err) {
									if (err) console.log(('Failed kafka message to '+topicName).red+' '+err);
								});
							}

							callback1(err);
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
								//this wraps quotes on strings
								c(JSON.stringify(item));
							}, function(resultDevices) {
								Models.Application.stateBucket.replace(sub, resultDevices.toString()+',', {cas: result.cas}, callback2);
							});
						}
					], c2);

				}, c1);
			}, callback1);
		},
		function deleteItems(result, callback1) {
			var query = cb.ViewQuery.from('dev_models', 'by_author').custom({inclusive_end: true, key: user.id});

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
			var query = cb.ViewQuery.from('dev_models', 'by_author_lookup').custom({inclusive_end: true, key: user.id});
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
