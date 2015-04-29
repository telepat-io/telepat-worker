var async = require('async');
var Models = require('octopus-models-api');
var cb = require('couchbase');
/*var gcm = require('node-gcm-service');
var apn = require('apn');*/
var mandrill = require('mandrill-api/mandrill');
var mandrillClient = new mandrill.Mandrill('NT-lxYd5VPu7CyyW2sduVg');

/*var pnSender = new gcm.Sender({key: 'apiKey'});
var apnConnection = new apn.Connection({cert : 'certificate', key: 'privatekey'});*/

function Write(message) {
    var key = message.key;
    var subscribedDevices = [];

    //I: get & delete delta
    //II: aggregate deltas
    //III: delete unique IDs

    async.waterfall([
        function getSubscribedDevices(callback) {
			var keyParts = key.split(':');
            keyParts.pop();

            stateBucket.get(keyParts.join(':'), function(err, result) {
                if (err && err.code == cb.errors.keyNotFound)
                    return callback();
                else if (err)
                    return callback(err);

 				if (!result.value)
					var devices = [];
				else
                	var devices = JSON.parse('['+result.value.slice(0, -1)+']');
                if (devices.length) {
                    Models.Subscription.multiGetDevices(devices, function(err1, results) {
                        if (err1) return callback(err1);

                        subscribedDevices = results;
                        callback();
                    });
                }

            });
        },
        function getDeltas(callback) {
            stateBucket.get(key, callback);
        },
		function removeDeltas(result, callback) {
			stateBucket.remove(key, function(err, results) {
				if (err && err.code !== cb.errors.keyNotFound) {
					callback(err);
				} else
					callback(null, result);
			});
		},
        function processDeltas(result, callback) {
            var deltas = result.value.deltas;

            var newItems = [];
            var modifiedItemsPatches = {};
            var deletedItems = {};

            async.each(deltas, function(d, callback) {
                switch (d.op) {
                    case "add": {
                        newItems.push({value: d.value, guid: d.guid});

                        break;
                    }
                    case "increment" : {
                        var pathParts = d.path.split('/');//model/id/field_name

                        if (modifiedItemsPatches[d.path])
                            modifiedItemsPatches[d.path].value += d.value;
                        else
                            modifiedItemsPatches[d.path] = {op: d.op, path: pathParts[2], guid: d.guid, value: d.value, email: d.email};

                        break;
                    }
                    case "replace": {
                        var pathParts = d.path.split('/');//model/id/field_name

                        modifiedItemsPatches[d.path] = {op: d.op, path: pathParts[2], guid: d.guid, value: d.value, email: d.email};

                        break;
                    }
                    case "delete": {
                        deletedItems[d.path] = d;

                        break
                    }
                }

                callback();
            }, function(err) {
                callback(null, newItems, modifiedItemsPatches, deletedItems);
            });
        },
        function updateDatabase(newItems, modifiedItemsPatches, deletedItems, callback) {
			var it = 0;
            var clientPatches = {'new': [], updated: [], removed: []};

            async.series([
                function CreateItems(callback1) {
					async.whilst(function() { return it < newItems.length; },
                        function(callback2) {
                            opIdentifiersBucket.remove(newItems[it].guid, function(err, result) {
                               	it++;
								if (err) {
                                   if (err.code == cb.errors.keyNotFound) {
                                       console.log(newItems[it-1].guid+' has already been removed.');
                                       callback2();
                                   }
                                   else
                                        callback2(err);
                               }
                               else {
                                   console.log(newItems[it-1].guid+' has been removed.');
                                   if (newItems[it-1].value.type == 'user') {
									   Models.User.create(newItems[it-1].value, callback2);
								   } else {
									   Models.Model.create(newItems[it - 1].value.type, message.applicationId, newItems[it - 1].value, function (err1, result1) {
										   if (err1) return callback2(err1);

										   var id = Object.keys(result1)[0];
										   result1[newItems[it - 1].value.type + id] = result1;
										   delete result1[id];

										   clientPatches.new.push({op: 'add', value: result1});
										   callback2();
									   });
								   }
                               }
                            });
                        },
                        callback1);
                },
                function UpdateItems(callback1) {
					async.each(Object.keys(modifiedItemsPatches), function(item, callback2){
                        var pathParts = item.split('/'); //model/id/fieldname

                        if (deletedItems[pathParts[0]+pathParts[1]]) {
							delete modifiedItemsPatches[item];
							return callback2();
						}

                        opIdentifiersBucket.remove(modifiedItemsPatches[item].guid, function(err, result) {
                            if (err) {
                                if (err.code == cb.errors.keyNotFound)
                                    callback2();
                                else
                                    callback2(err);
                            }
                            else {
								if (message.user) {
									Models.User.update(modifiedItemsPatches[item].email, [modifiedItemsPatches[item]], callback2);
								} else {
									Models.Model.update(pathParts[0], message.applicationId, pathParts[1], [modifiedItemsPatches[item]], callback2);
								}
                            }
                        });
                    }, function(err) {
						clientPatches.updated = modifiedItemsPatches;
						callback1(err);
					});
                },
                function DeleteItems(callback1) {
					clientPatches.removed = deletedItems;

                    async.each(Object.keys(deletedItems), function(delPath, callback2){
                        var pathParts = delPath.split('/');//model/id

						opIdentifiersBucket.remove(deletedItems[delPath].guid, function(err, result) {
                            if (err) {
								if (err.code == cb.errors.keyNotFound)
                                    callback2();
                                else
                                    callback2(err);
                            }
                            else {
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
							}
                        });
                    }, callback1);
                },
				function updateObjectCount(callback1) {
					var countKey = key.split(':');
					//remove 'deltas' from key name
					countKey.pop();
					countKey = countKey.join(':');
					var objectsCount = newItems.length - deletedItems.length;

					stateBucket.counter(countKey, objectsCount, {initial: 0}, callback1);
				}
            ], function(err) {
                if (err) return callback(err);

				//var pushNotificationDeviceTokens = {android: [], ios: []};

                async.each(Object.keys(subscribedDevices), function(d, callback1) {
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

                        mandrillClient.messages.send({message: message, async: true}, function(result) {console.log(result);}, function(e) {console.log('A mandrill error occurred: ' + e.name + ' - ' + e.message);});
                    }/* else {
						pushNotificationDeviceTokens[subscribedDevices[d].os].push(subscribedDevices[d].user_token);
					}*/
                    callback1();
                });

				/*var gcmMessage = {data: clientPatches};

				pnSender.sendMessage(gcmMessage.toJSON(), pushNotificationDeviceTokens.android, true, function(err, data) {

				});

				var apnNotification = new apn.Notification();
				apnNotification.payload = clientPatches;

				async.each(Object.keys(pushNotificationDeviceTokens.ios), function(device, c){
					pushNotificationDeviceTokens.ios[device] = new apn.Device(pushNotificationDeviceTokens.ios[device]);
					c();
				});

				apnConnection.pushNotification(apnNotification, pushNotificationDeviceTokens.ios);*/

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
