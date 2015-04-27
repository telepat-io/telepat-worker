var async = require('async');
var Models = require('octopus-models-api');
var cb = require('couchbase');
var uuid = require('uuid');

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
						if (result === false) {
							stateBucket.insert(subKey, {deltas: [{op: 'add', value: userProps, guid: guid}]}, function(err, res1) {
								opIdentifiersBucket.upsert(guid, "", function(err2, res2) {
									if (err2) return callback(err2);

									kafkaProducer.send([{
										topic: 'write',
										messages: [JSON.stringify({key: subKey, applicationId: appId})],
										attributes: 0,
										applicationId: appId
									}], callback);
								});
							});
						} else {
							var items = result.value;
							items.deltas.push({op: 'add', value: userProps, guid: guid});
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
				var guid = uuid.v4();
				var appId = message.applicationId;
				var id = message.id;
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
						function writeDeltas(result, callback) {
							if (result === false) {
								stateBucket.insert(subKey, {deltas: patch}, function(err, res1) {
									opIdentifiersBucket.upsert(guid, "", function(err2, res2) {
										if (err2) return callback(err2);

										kafkaProducer.send([{
											topic: 'write',
											messages: [JSON.stringify({key: subKey, applicationId: appId})],
											attributes: 0
										}], callback);
									});
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

				async.each(subKey, function(subKey, c) {
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
								stateBucket.insert(subKey, {deltas: [{op: 'delete', path: 'user/'+id, guid: guid}]}, function(err, res1) {
									opIdentifiersBucket.upsert(guid, "", function(err2, res2) {
										if (err2) return callback(err2);

										kafkaProducer.send([{
											topic: 'write',
											messages: [JSON.stringify({key: subKey, applicationId: appId})],
											attributes: 0,
											applicationId: appId,
											user: true
										}], callback);
									});
								});
							} else {
								var items = result.value;
								items.deltas = items.deltas.push({op: 'delete', path: 'user/'+id, guid: guid});
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
							stateBucket.insert(deltasKey, {deltas: [{op: 'delete', path: 'context/'+context_id, guid: guid}]}, function(err, res1) {
								opIdentifiersBucket.upsert(guid, "", function(err2, res2) {
									if (err2) return callback(err2);

									kafkaProducer.send([{
										topic: 'write',
										messages: [JSON.stringify({key: deltasKey, applicationId: appId, context: true})],
										attributes: 0
									}], callback);
								});
							});
						} else {
							var items = result.value;
							items.deltas.push({op: 'delete', path: 'context/'+context_id, guid: guid});
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
		switch(message.op) {
			case 'add': {
				var item = message.object;
				var mdl = item.type;
				var context = item.context_id;
				var userId = item.user_id;
				var parent = null;

				for (var r in Models.Application.loadedAppModels[message.applicationId][mdl].belongsTo) {
					if (item[Models.Application.loadedAppModels[message.applicationId][mdl].belongsTo[r].parentModel+'_id']) {
						parent = {model: Models.Application.loadedAppModels[message.applicationId][mdl].belongsTo[r].parentModel,
							id: item[Models.Application.loadedAppModels[message.applicationId][mdl].belongsTo[r].parentModel+'_id']};
					}
				}

				var guid = uuid.v4();

				async.each(global.formKeys(message.applicationId, mdl, context, userId, parent),
					function(key, callback) {
						if (key) {
							stateBucket.getAndLock(key, function(err, results){
								if (err && err.code == cb.errors.keyNotFound) {
									stateBucket.insert(key, {deltas: [{op: 'add', value: item, guid: guid}]}, function(err1, results1) {
										if (err1) return callback(err1);

										opIdentifiersBucket.upsert(guid, "", function(err2, result) {
											if (err2) return callback(err2);

											kafkaProducer.send([{
												topic: 'write',
												messages: [JSON.stringify({key: key, applicationId: message.applicationId})],
												attributes: 0,
												applicationId: message.applicationId
											}], callback);
										});

									});
								} else if (err)
									return callback(err);
								else {
									var items = results.value;
									items.deltas.push({op: 'add', value: item, guid: guid});

									stateBucket.replace(key, items, {cas: results.cas}, callback);
								}
							});
						} else
							callback(null);
					},
					function(err){
						if (err) console.log(err);
					});

				break;
			}

			case 'edit': {
				//var context = message.context;
				var patch = message.object;
				var id = message.id;
				var mdl = message.type;

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
						var context = result.context_id;
						var userId = result.user_id;
						var parent = null;

						for (var r in Models.Application.loadedAppModels[message.applicationId][mdl].belongsTo) {
							if (result[Models.Application.loadedAppModels[message.applicationId][mdl].belongsTo[r].parentModel+'_id']) {
								parent = {model: Models.Application.loadedAppModels[message.applicationId][mdl].belongsTo[r].parentModel,
									id: result[Models.Application.loadedAppModels[message.applicationId][mdl].belongsTo[r].parentModel+'_id']};
							}
						}

						async.each(global.formKeys(message.applicationId, mdl, context, userId, parent, id),
							function(key, callback1) {
								if (key) {
									stateBucket.getAndLock(key, function(err, results){
										if (err && err.code == cb.errors.keyNotFound) {
											stateBucket.insert(key, {deltas: patch}, function(err1, results1) {
												if (err1) return callback1(err1);

												kafkaProducer.send([{
													topic: 'write',
													messages: [JSON.stringify({key: key, applicationId: message.applicationId})],
													attributes: 0,
													applicationId: message.applicationId
												}], callback1);

											});
										} else if (err)
											return callback1(err);
										else {
											var items = results.value;
											items.deltas = items.deltas.concat(patch);

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

			case 'delete': {
				var id = message.object.id;
				var mdl = message.object.type;

				async.waterfall([
					function(callback) {
						new Models.Model(mdl, message.applicationId, id, callback);
					},
					function(result, callback) {
						var userId = result.user_id;
						var context = result.context_id;
						var parent = null;

						for (var r in Models.Application.loadedAppModels[message.applicationId][mdl].belongsTo) {
							if (result[Models.Application.loadedAppModels[message.applicationId][mdl].belongsTo[r].parentModel+'_id'])
								parent = {model: Models.Application.loadedAppModels[message.applicationId][mdl].belongsTo[r].parentModel,
									id: result[Models.Application.loadedAppModels[message.applicationId][mdl].belongsTo[r].parentModel+'_id']};
						}

						var guid = uuid.v4();

						async.each(global.formKeys(message.applicationId, mdl, context, userId, parent, id),
							function(key, callback1) {
								console.log(key);
								if (key) {
									stateBucket.getAndLock(key, function(err, results){
										if (err && err.code == cb.errors.keyNotFound) {
											stateBucket.insert(key, {deltas: [{op: 'delete', path: mdl+'/'+id, guid: guid}]}, function(err1, results1) {
												if (err1) return callback1(err1);

												opIdentifiersBucket.upsert(guid, "", function(err2, result) {
													if (err2) return callback1(err2);

													kafkaProducer.send([{
														topic: 'write',
														messages: [JSON.stringify({key: key, applicationId: message.applicationId})],
														attributes: 0,
														applicationId: message.applicationId
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
