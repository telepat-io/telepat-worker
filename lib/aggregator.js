var async = require('async');
var Models = require('octopus-models-api');
var cb = require('couchbase');
var uuid = require('uuid');

/**
 * The aggregator receives messages from the API that describe changes in the database (create/update/delete).
 * It writes these changes in the deltas documents in the state document. Each operation has an uniqued identifier which
 * helps writers determine if the operation has already been carried out since multiple subscriptions may have the same
 * delta (the same changed object).
 *
 */
function Aggregate(message) {
    if (message.user || message.object.type == 'user') {
		switch(message.op) {
			case 'add': {
				var userProps = message.object;
				var subKey = 'blg:users:deltas';
				var guid = uuid.v4();
				var appId = message.object.application_id;

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
						/**
						 * If the deltas key didn't exist, add curent delta and send kafka message to writer
						 * If key already existed append delta to document, don't send message to kafka since 1 message
						 * from the first delta inserted is enough to notice the writer of new deltas to write
						 */
						if (result === false) {
							stateBucket.insert(subKey, {deltas: [{op: 'add', value: userProps, guid: guid, type: 'user'}]}, function(err, res1) {
								opIdentifiersBucket.upsert(guid, "", function(err2, res2) {
									if (err2) return callback(err2);

									kafkaProducer.send([{
										topic: 'write',
										messages: [JSON.stringify({key: subKey, applicationId: appId, user: true, guid: guid})],
										attributes: 0
									}], callback);
								});
							});
						} else {
							var items = result.value;
							items.deltas.push({op: 'add', value: userProps, guid: guid, type: 'user'});
							stateBucket.replace(subKey, items, {cas: result.cas}, callback);
						}
					}
				], function(err, result) {
					if (err) console.log(err);
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
					opIdentifiersBucket.upsert(patch[p].guid, "", function(err, result) {if(err) console.log('User edit operation' +
						' identifier add: ', err);});
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
								stateBucket.insert(subKey, {deltas: patch}, function(err, res1) {
									if (err) return callback(err);

									kafkaProducer.send([{
										topic: 'write',
										messages: [JSON.stringify({key: subKey, applicationId: appId, user: true})]
									}], callback);
								});
							} else {
								var items = result.value;
								items.deltas = items.deltas.concat(patch);
								stateBucket.replace(subKey, items, {cas: result.cas}, callback);
							}
						}
					], c);
				}, function(err) {
					if (err) console.log(err);
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
								stateBucket.insert(subKey, {deltas: [{op: 'delete', path: 'user/'+id, guid: guid, type: 'user'}]}, function(err, res1) {
									opIdentifiersBucket.upsert(guid, "", function(err2, res2) {
										if (err2) return callback(err2);

										kafkaProducer.send([{
											topic: 'write',
											messages: [JSON.stringify({key: subKey, applicationId: appId, user: true})]
										}], callback);
									});
								});
							} else {
								var items = result.value;
								items.deltas = items.deltas.push({op: 'delete', path: 'user/'+id, guid: guid, type: 'user'});
								stateBucket.replace(subKey, items, {cas: result.cas}, callback);
							}
						}
					], c);
				}, function(err, result) {
					if (err) console.log(err);
				});

				break;
			}
		}

	} else if (message.context) {
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
							stateBucket.insert(deltasKey, {deltas: [{op: 'delete', path: 'context/'+context_id, guid: guid, type: 'context'}]}, function(err, res1) {
								if (err) return callback(err);

								opIdentifiersBucket.upsert(guid, "", function(err2, res2) {
									if (err2) return callback(err2);

									kafkaProducer.send([{
										topic: 'write',
										messages: [JSON.stringify({key: deltasKey, applicationId: appId, context: true})]
									}], callback);
								});
							});
						} else {
							var items = result.value;
							items.deltas.push({op: 'delete', path: 'context/'+context_id, guid: guid, type: 'context'});
							stateBucket.replace(deltasKey, items, {cas: result.cas}, callback);
						}
					}
				], function(err, result) {
					if (err) console.log(err);
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
				var userId = item.user_id;
				var parent = null;

				var guid = uuid.v4();

				async.waterfall([
					function(callback) {
						global.formKeys(message.applicationId, context, item, callback);
					},
					function(results, callback) {
						async.each(results,
							function(key, callback1) {
								if (key) {
									stateBucket.getAndLock(key, function(err, results1){
										if (err && err.code == cb.errors.keyNotFound) {
											stateBucket.insert(key, {deltas: [{op: 'add', value: item, guid: guid}]}, function(err1, results2) {
												if (err1) return callback1(err1);

												opIdentifiersBucket.upsert(guid, "", function(err2, result) {
													if (err2) return callback1(err2);

													kafkaProducer.send([{
														topic: 'write',
														messages: [JSON.stringify({key: key, applicationId: message.applicationId})]
													}], callback1);
												});

											});
										} else if (err)
											return callback1(err);
										else {
											var items = results1.value;
											items.deltas.push({op: 'add', value: item, guid: guid});

											stateBucket.replace(key, items, {cas: results1.cas}, callback1);
										}
									});
								} else
									callback1(null);
							}, callback);
					}
				], function(err) {
					console.log(err);
				});

				break;
			}

			case 'edit': {
				var patch = message.object;
				var id = message.id;
				var mdl = message.type;
				var context = message.context;

				async.waterfall([
					function(callback) {
						async.each(Object.keys(patch), function(p, callback2) {
							var guid = uuid.v4();
							patch[p].guid = guid;
							opIdentifiersBucket.upsert(guid, "", callback2);
						}, callback);
					},
					function(callback) {
						new Models.Model(mdl, message.applicationId, id, callback);
					},
					function(result, callback) {
						global.formKeys(message.applicationId, context, result, callback);
					},
					function(results, callback) {
						async.each(results,
							function(key, callback1) {
								if (key) {
									stateBucket.getAndLock(key, function(err, results1){
										if (err && err.code == cb.errors.keyNotFound) {
											stateBucket.insert(key, {deltas: patch}, function(err1, results2) {
												if (err1) return callback1(err1);

												kafkaProducer.send([{
													topic: 'write',
													messages: [JSON.stringify({key: key, applicationId: message.applicationId})]
												}], callback1);

											});
										} else if (err)
											return callback1(err);
										else {
											var items = results1.value;
											items.deltas = items.deltas.concat(patch);

											stateBucket.replace(key, items, {cas: results1.cas}, callback1);
										}
									});
								} else
									callback1();
							}, callback
						);
					},
				], function(err, results) {
					if (err) console.log(err);
				});

				break;
			}

			case 'delete': {
				var id = message.object.id;
				var mdl = message.object.type;

				var guid = uuid.v4();

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
									stateBucket.getAndLock(key, function(err, results){
										if (err && err.code == cb.errors.keyNotFound) {
											stateBucket.insert(key, {deltas: [{op: 'delete', path: mdl+'/'+id, guid: guid}]}, function(err1, results1) {
												if (err1) return callback1(err1);

												opIdentifiersBucket.upsert(guid, "", function(err2, result) {
													if (err2) return callback1(err2);

													kafkaProducer.send([{
														topic: 'write',
														messages: [JSON.stringify({key: key, applicationId: message.applicationId})]
													}], callback1);
												});
											});
										} else if (err)
											return callback1(err);
										else {
											var items = results.value;
											items.deltas = items.deltas.push({op: 'delete', path: mdl+'/'+id, guid: guid});

											stateBucket.replace(key, items, {cas: results.cas}, callback1);
										}
									});
								} else
									callback1();
							}, callback
						);
					}
				], function(err, results) {
					if (err) console.log(err);
				});

				break;
			}
		}
	}
}

module.exports = Aggregate;
