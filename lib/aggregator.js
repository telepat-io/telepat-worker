var async = require('async');
var Models = require('octopus-models-api');
var cb = require('couchbase');

function Aggregate(message) {
    switch(message.op) {
        case 'add': {
            var item = message.object;
            var mdl = item.type;
            var context = item.context_id;
            var userId = item.user_id;
            var parent = null;

            for (var r in Models.Model._spec[mdl].belongsTo) {
                if (item[Models.Model._spec[mdl].belongsTo[r].parentModel+'_id']) {
                    parent = {model: Models.Model._spec[mdl].belongsTo[r].parentModel, id: item[Models.Model._spec[mdl].belongsTo[r].parentModel+'_id']};
                }
            }

            async.each(global.formKeys(mdl, context, userId, parent),
                function(key, callback) {
                    console.log(key);
                    if (key) {
                        stateBucket.getAndLock(key, function(err, results){
                            if (err && err.code == cb.errors.keyNotFound) {
                                stateBucket.insert(key, {deltas: [{op: 'add', value: item}]}, function(err1, results1) {
                                    if (err1) return callback(err1);

                                    kafkaProducer.send([{
                                        topic: 'write',
                                        messages: [JSON.stringify({key: key})],
                                        attributes: 0
                                    }], callback);
                                });
                            } else if (err)
                                return callback(err);
                            else {
                                var items = results.value;
                                items.deltas.push({op: 'add', value: item});

                                stateBucket.replace(key, items, {cas: results.cas}, callback);
                            }
                        });
                    } else
                        callback(null);
                },
                function(err){
                    if (err) console.log(err);
                });

            /*Models.Model.create(mdl, context, item, userId, parent, function(err, results) {
             if (err) return console.log(err);

             console.log(results);
             });*/

            break;
        }

        case 'edit': {
            //var context = message.context;
            var patch = message.object;
            var id = message.id;
            var mdl = message.type;

            async.waterfall([
                function(callback) {
                    new Models.Model(mdl, id, callback);
                },
                function(result, callback) {
                    var context = result.context_id;
                    var userId = result.user_id;
                    var parent = null;

                    for (var r in Models.Model._spec[mdl].belongsTo) {
                        if (result[Models.Model._spec[mdl].belongsTo[r].parentModel+'_id']) {
                            parent = {model: Models.Model._spec[mdl].belongsTo[r].parentModel, id: result[Models.Model._spec[mdl].belongsTo[r].parentModel+'_id']};
                        }
                    }

                    async.each(global.formKeys(mdl, context, userId, parent),
                        function(key, callback1) {
                            if (key) {
                                stateBucket.getAndLock(key, function(err, results){
                                    if (err && err.code == cb.errors.keyNotFound) {
                                        stateBucket.insert(key, {deltas: patch}, function(err1, results1) {
                                            if (err1) return callback1(err1);

                                            kafkaProducer.send([{
                                                topic: 'write',
                                                messages: [JSON.stringify({key: key})],
                                                attributes: 0
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

            /*Models.Model.update(mdl, context, id, patch, function(err, results) {
             if (err) return console.log(err);

             console.log(results);
             });*/

            break;
        }

        case 'delete': {
            var id = message.object.id;
            var mdl = message.object.type;

            async.waterfall([
                function(callback) {
                    new Models.Model(mdl, id, callback);
                },
                function(result, callback) {
                    var userId = result.user_id;
                    var context = result.context_id;
                    var parent = null;

                    for (var r in Models.Model._spec[mdl].belongsTo) {
                        if (result[Models.Model._spec[mdl].belongsTo[r].parentModel+'_id'])
                            parent = {model: Models.Model._spec[mdl].belongsTo[r].parentModel, id: result[Models.Model._spec[mdl].belongsTo[r].parentModel+'_id']};
                    }

                    async.each(global.formKeys(mdl, context, userId, parent),
                        function(key, callback1) {
                            console.log(key);
                            if (key) {
                                stateBucket.getAndLock(key, function(err, results){
                                    if (err && err.code == cb.errors.keyNotFound) {
                                        stateBucket.insert(key, {deltas: [{op: 'delete', path: mdl+'/'+id}]}, function(err1, results1) {
                                            if (err1) return callback1(err1);

                                            kafkaProducer.send([{
                                                topic: 'write',
                                                messages: [JSON.stringify({key: key})],
                                                attributes: 0
                                            }], callback1);
                                        });
                                    } else if (err)
                                        return callback1(err);
                                    else {
                                        var items = results.value;
                                        items.deltas = items.deltas.push({op: 'delete', path: mdl+'/'+id});

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

            /*Models.Model.delete(mdl, context, id, function(err, results) {
             if (err) return console.log(err);

             console.log(results);
             });*/

            break;
        }
    }
}

module.exports = Aggregate;