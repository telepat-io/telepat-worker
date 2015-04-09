var async = require('async');
var Models = require('octopus-models-api');
var cb = require('couchbase');

function Write(message) {
    //console.log(message);
    var key = message.key;

    //I: get & delete delta
    //II: aggregate deltas
    //III: delete unique IDs

    async.waterfall([
        function getDeltas(callback) {
            stateBucket.get(key, callback);
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
                            modifiedItemsPatches[d.path] = {op: d.op, path: pathParts[2], guid: d.guid, value: d.value};

                        break;
                    }
                    case "replace": {
                        var pathParts = d.path.split('/');//model/id/field_name

                        modifiedItemsPatches[d.path] = {op: d.op, path: pathParts[2], guid: d.guid, value: d.value};

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

            async.series([
                function CreateItems(callback1) {
                    async.whilst(function() { return it < newItems.length; },
                        function(callback2) {
                            opIdentifiersBucket.remove(newItems[it].guid, function(err, result) {
                               if (err) {
                                   it++;
                                   if (err.code == cb.errors.keyNotFound) {
                                       console.log(newItems[it-1].guid+' has already been removed.');
                                       callback2();
                                   }
                                   else
                                        callback2(err);
                               }
                               else {
                                   it++;
                                   console.log(newItems[it-1].guid+' has been removed.');
                                   Models.Model.create(newItems[it-1].value.type, newItems[it-1].value, callback2);
                               }
                            });
                        },
                        callback1);
                },
                function UpdateItems(callback1) {
                    async.each(Object.keys(modifiedItemsPatches), function(item, callback2){
                        var pathParts = item.split('/'); //model/id/fieldname

                        if (deletedItems[pathParts[0]+pathParts[1]])
                            return callback2();

                        opIdentifiersBucket.remove(modifiedItemsPatches[item].guid, function(err, result) {
                            if (err) {
                                if (err.code == cb.errors.keyNotFound)
                                    callback2();
                                else
                                    callback2(err);
                            }
                            else {
                                Models.Model.update(pathParts[0], pathParts[1], [modifiedItemsPatches[item]], callback2);
                            }
                        });
                    }, callback1);
                },
                function DeleteItems(callback1) {
                    async.each(Object.keys(deletedItems), function(delPath, callback2){
                        var pathParts = delPath.split('/');//model/id

                        opIdentifiersBucket.remove(deletedItems[delPath].guid, function(err, result) {
                            if (err) {
                                if (err.code == cb.errors.keyNotFound)
                                    callback2();
                                else
                                    callback2(err);
                            }
                            else
                                Models.Model.delete(pathParts[0], pathParts[1], callback2);
                        });
                    }, callback1);
                }
            ], function(err) {
                stateBucket.remove(key, function(err, results) {
                    if (err && err.code !== cb.errors.keyNotFound) {
                        callback(err);
                    } else
                        callback();
                });
            }) ;
        }
    ], function(err, results) {
        if (err) console.log(err);
    });

    /*memcachedClient.delete(message.guid, function(err, results) {
        console.log(err, results);
        if (results === false)
            return console.log("'"+message.guid+"' has already been processed.");
        else {
            async.waterfall([
                function getDeltas(callback) {
                    stateBucket.get(key, callback);
                },
                function processDeltas(result, callback) {
                    var deltas = result.value.deltas;

                    var newItems = [];
                    var modifiedItemsPatches = {};
                    var deletedItems = {};

                    for (var i in deltas) {
                        switch (deltas[i].op) {
                            case "add": {
                                newItems.push(deltas[i].value);

                                break;
                            }
                            case "increment" :
                            case "replace": {
                                var pathParts = deltas[i].path.split('/');//model/id/field_name
                                var id = pathParts[1].toString();

                                if (!modifiedItemsPatches[pathParts[0]+"/"+id])
                                    modifiedItemsPatches[pathParts[0]+"/"+id] = [];

                                modifiedItemsPatches[pathParts[0]+"/"+id].push({op: deltas[i].op, path: pathParts[2], value: deltas[i].value});

                                break;
                            }
                            case "delete": {
                                deletedItems[deltas[i].path] = true;

                                break
                            }
                        }
                    }

                    callback(null, newItems, modifiedItemsPatches, deletedItems);
                },
                function updateDatabase(newItems, modifiedItemsPatches, deletedItems, callback) {
                    var it = 0;

                    async.series([
                        function CreateItems(callback1) {
                            async.whilst(function() { return it < newItems.length; },
                                function(callback2) {
                                    Models.Model.create(newItems[it].type, newItems[it], function(err, results) {
                                        if (err) return callback2(err);
                                        it++;
                                        callback2();
                                    });
                                },
                                callback1);
                        },
                        function UpdateItems(callback1) {
                            async.each(Object.keys(modifiedItemsPatches), function(item, callback2){
                                var pathParts = item.path.split('/'); //model/id

                                if (deletedItems[pathParts[0]+pathParts[1]])
                                    return callback2();

                                Models.Model.update(pathParts[0], pathParts[1], modifiedItemsPatches[item], callback2);
                            }, callback1);
                        },
                        function DeleteItems(callback1) {
                            async.each(Object.keys(deletedItems), function(item, callback2){
                                var pathParts = item.path.split('/');//model/id

                                Models.Model.delete(pathParts[0], pathParts[1], callback2);
                            }, callback1);
                        }
                    ], function(err) {
                        callback(err);
                    }) ;
                }
            ], function(err, results) {
                if (err) {
                    throw err;
                }
            });
        }
    });*/
}

module.exports = Write;