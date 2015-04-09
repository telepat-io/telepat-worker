var async = require('async');
var Models = require('octopus-models-api');
var cb = require('couchbase');
var gcm = require('node-gcm-service');
var mandrill = require('mandrill-api/mandrill');
var mandrillClient = new mandrill.Mandrill('NT-lxYd5VPu7CyyW2sduVg');

//var pnSender = new gcm.Sender({key: 'apiKey'});

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
            var clientPatches = {'new': [], updated: [], removed: []};

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
                                   Models.Model.create(newItems[it-1].value.type, newItems[it-1].value, function(err1, result1) {
                                       if (err1) return callback2(err1);

                                       var id = Object.keys(result1)[0];
                                       result1[newItems[it-1].value.type+id] = result1;
                                       delete result1[id];

                                       clientPatches.new.push({op: 'add', value: result1});
                                   });
                               }
                            });
                        },
                        callback1);
                },
                function UpdateItems(callback1) {
                    clientPatches.updated =modifiedItemsPatches;

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
                            else
                                Models.Model.delete(pathParts[0], pathParts[1], callback2);
                        });
                    }, callback1);
                }
            ], function(err) {
                if (err) return callback(err);

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
                    }
                    callback1();
                });

                stateBucket.remove(key, function(err1, results) {
                    if (err1 && err1.code !== cb.errors.keyNotFound) {
                        callback(err1);
                    } else
                        callback();
                });
            }) ;
        }
    ], function(err, results) {
        if (err) {
            console.log(err);
            console.trace('Writer stack');
        }
    });
}

module.exports = Write;