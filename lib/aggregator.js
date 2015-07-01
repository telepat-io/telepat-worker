var async = require('async');
var Models = require('octopus-models-api');
var cb = require('couchbase');
var uuid = require('uuid');

/**
 * The aggregator receives messages from the API that describe changes in the database (create/update/delete).
 * It writes these changes in the deltas documents in the state document.
 */
function Aggregate(message) {
	if (message.isUser === true) {
		switch(message.op) {
			case 'add': {
				var userProps = message.object;
				var subKey = 'blg:users:deltas';
				var guid = uuid.v4();
				var appId = message.object.application_id;
				var keysWithNewDeltas = [];

				async.waterfall([
					function (callback) {
						redisClient.rpush([subKey, JSON.stringify({op: 'add', value: userProps, type: 'user', guid: guid, ts: process.hrtime().join('') })], function(err, deltasLength) {
							if (err) {
								console.log('Failed'.red+' to push delta object to key '+subKey);
								return callback();
							}

							if (deltasLength == 1)
								keysWithNewDeltas.push(subKey);

							callback();
						});
					}
				], function(err, result) {
					if (err) return console.log(err);

					if (keysWithNewDeltas.length > 0) {
						kafkaProducer.send([{
							topic: 'write',
							messages: [JSON.stringify({keys: [subKey], applicationId: appId, user: true})],
							attributes: 0
						}], function(err, result) {
							if (err) console.log(err);
						});
					}
				});

				break;
			}

			case 'edit': {
				var patch = message.object;
				var appId = message.applicationId;
				var id = message.id;
				var subKeys = ['blg:users:deltas', 'blg:users:'+id+':deltas'];

				for(var p in patch) {
					patch[p].type = 'user';
					patch[p].guid = uuid.v4();
				}

				async.each(subKeys, function(subKey, c) {
					async.waterfall([
						function getDeltas(callback) {
							stateBucket.getAndLock(subKey, function(err, result) {
								if (err && err.code == cb.errors.keyNotFound)
									callback(null, false);
								else if (err)
									callback(err);
								else
									callback(null, result);
							});
						},
						function writeDeltas(result, callback) {
							if (result === false) {
								stateBucket.insert(subKey, {deltas: patch}, callback);
							} else {
								var items = result.value;
								items.deltas = items.deltas.concat(patch);
								stateBucket.replace(subKey, items, {cas: result.cas}, callback);
							}
						}
					], c);
				}, function(err) {
					if (err) return console.log(err);

					kafkaProducer.send([{
						topic: 'write',
						messages: [JSON.stringify({keys: subKeys, applicationId: appId, user: true})]
					}], function(err, result) {
						if (err) console.log(err);
					});
				});

				break;
			}

			case 'delete': {
				var id = message.id;
				var guid = uuid.v4();
				var subKeys = ['blg:users:deltas', 'blg:users:'+id+':deltas'];

				async.each(subKeys, function(subKey, c) {
					async.waterfall([
						function getDeltas(callback) {
							stateBucket.getAndLock(subKey, function(err, result) {
								if (err && err.code == cb.errors.keyNotFound)
									callback(null, false);
								else if (err)
									callback(err);
								else
									callback(null, result);
							});
						},
						function(result, callback) {
							if (result === false) {
								stateBucket.insert(subKey, {deltas: [{op: 'delete', path: 'user/'+id, guid: guid, type: 'user'}]}, callback);
							} else {
								var items = result.value;
								items.deltas = items.deltas.push({op: 'delete', path: 'user/'+id, guid: guid, type: 'user'});
								stateBucket.replace(subKey, items, {cas: result.cas}, callback);
							}
						}
					], c);
				}, function(err, result) {
					if (err) return console.log(err);

					kafkaProducer.send([{
						topic: 'write',
						messages: [JSON.stringify({keys: subKeys, applicationId: appId, user: true})]
					}], function(err, result) {
						if (err) console.log(err);
					});
				});

				break;
			}
		}

	} else if (message.isContext === true) {
		switch (message.op) {
			case 'delete': {
				var context_id = message.object.id;
				var deltasKey = 'blg:context:'+context_id+':deltas';
				var guid = uuid.v4();

				async.waterfall([
					function getDeltas(callback) {
						stateBucket.getAndLock(deltasKey, function(err, result) {
							if (err && err.code == cb.errors.keyNotFound)
								callback(null, false);
							else if (err)
								callback(err);
							else
								callback(null, result);
						});
					},
					function(result, callback) {
						if (result === false) {
							stateBucket.insert(deltasKey, {deltas: [{op: 'delete', path: 'context/'+context_id, guid: guid, type: 'context'}]}, callback);
						} else {
							var items = result.value;
							items.deltas.push({op: 'delete', path: 'context/'+context_id, guid: guid, type: 'context'});
							stateBucket.replace(deltasKey, items, {cas: result.cas}, callback);
						}
					}
				], function(err, result) {
					if (err) return console.log(err);

					kafkaProducer.send([{
						topic: 'write',
						messages: [JSON.stringify({keys: [deltasKey], applicationId: appId, context: true})]
					}], function(err, result) {
						if (err) console.log(err);
					});
				});

				break;
			}
		}
	} else {
		//Application objects
		switch(message.op) {
			case 'add': {
				var item = message.object;
				var mdl = item.type;
				var context = item.context_id;

				var guid = uuid.v4();

				async.waterfall([
					function(callback) {
						global.formKeys(message.applicationId, context, item, callback);
					},
					function(results, callback) {
						var keysWithNewDeltas = [];

						async.each(results, function(key, callback1){
							if (key) {
								redisClient.rpush([key, JSON.stringify({op: 'add', value: item, guid: guid, ts: process.hrtime().join('')})], function(err, deltasLength) {
									if (err) {
										console.log('Failed'.red+' to push delta object to key '+key);
										return callback1();
									}

									if (deltasLength == 1)
										keysWithNewDeltas.push(key);

									callback1();
								});
							} else
								callback1(null);
						}, function() {
							kafkaProducer.send([{
								topic: 'write',
								messages: [JSON.stringify({keys: keysWithNewDeltas, applicationId: message.applicationId})]
							}], callback);
						});
					}
				], function(err) {
					if (err) console.log(err);
				});

				break;
			}

			case 'edit': {
				var patch = message.object;
				var id = message.id;
				var mdl = message.type;
				var context = message.context;
				var updateTimestamp = message.ts;
				var updateTimestampBaseKey = 'blg:'+message.applicationId+':'+mdl+':'+id+':';

				async.waterfall([
					function(callback) {
						async.each(Object.keys(patch), function(p, callback1) {
							patch[p].guid = uuid.v4();
							patch[p].ts = process.hrtime().join('');
							callback1();
						}, callback);
					},
					function(callback) {
						new Models.Model(mdl, message.applicationId, id, callback);
					},
					function(result, callback) {
						global.formKeys(message.applicationId, context, result, callback);
					},
					function verifyPatchTimestamp(formKeysResult, callback) {
						async.filter(patch, function(itemPatch, filterCallback) {
							var modifiedProperty = itemPatch.path.split('/')[2]; //fieldname
							redisClient.get(updateTimestampBaseKey+':'+modifiedProperty+':mod_timestamp', function(err, result) {
								var wholeKey = updateTimestampBaseKey+':'+modifiedProperty+':mod_timestamp';
								if (result === null) {
									redisClient.set(wholeKey, updateTimestamp);
									redisClient.expire(wholeKey, 60);

									return filterCallback(true);
								}
								if (parseInt(result) < updateTimestamp) {
									redisClient.set(wholeKey, updateTimestamp, function(err, result) {
										redisClient.expire(wholeKey, 60);
									});

									return filterCallback(true);
								}

								return filterCallback(false);
							});
						}, function(results) {
							if (!results.length) {
								return callback(new Error('All patches has been discarded due to being outdated.'));
							}

							patch = results;
							callback(null, formKeysResult);
						});
					},
					function(results, callback) {
						var keysWithNewDeltas = [];

						async.each(results,
							function(key, callback1) {
								if (key) {
									var operationArgs = [key];

									patch.map(function(patchValue, patchIndex, originalArray) {
										originalArray[patchIndex] = JSON.stringify(patchValue);
									});

									operationArgs = operationArgs.concat(patch);

									redisClient.rpush(operationArgs, function(err, deltasLength) {
										if (err) {
											console.log('Failed'.red+' to push delta object to key '+key);
											return callback1();
										}

										if (deltasLength == 1)
											keysWithNewDeltas.push(key);

										callback1();
									});
								} else
									callback1();
							}, function(err1) {
								if (err1) return callback(err1);

								if (keysWithNewDeltas.length) {
									kafkaProducer.send([{
										topic: 'write',
										messages: [JSON.stringify({keys: keysWithNewDeltas, applicationId: message.applicationId})]
									}], function(err) {
										callback();
									});
								} else {
									callback();
								}
							}
						);
					}
				], function(err) {
					if (err) console.log(err);
				});

				break;
			}

			case 'delete': {
				var id = message.object.id;
				var mdl = message.object.type;
				var keysWithNewDeltas = [];

				async.waterfall([
					function(callback) {
						new Models.Model(mdl, message.applicationId, id, callback);
					},
					function(result, callback) {
						global.formKeys(message.applicationId, result.context_id, result, callback);
					},
					function(result, callback) {
						async.each(result,
							function(key, callback1) {
								if (key) {
									redisClient.rpush([key, JSON.stringify({op: 'delete', path: mdl+'/'+id, guid: guid, ts: process.hrtime().join('')})], function(err, deltasLength) {
										if (err) {
											console.log('Failed'.red+' to push delta object to key '+key);
											return callback1();
										}

										if (deltasLength == 1)
											keysWithNewDeltas.push(key);

										callback1();
									});
								} else
									callback1();
							}, function() {
								kafkaProducer.send([{
									topic: 'write',
									messages: [JSON.stringify({keys: keysWithNewDeltas, applicationId: message.applicationId})]
								}], callback);
							}
						);
					}
				], function(err) {
					if (err) console.log(err);
				});

				break;
			}
		}
	}
}

module.exports = Aggregate;
