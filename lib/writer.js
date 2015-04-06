var async = require('async');
var Models = require('octopus-models-api');
var cb = require('couchbase');

function Write(message) {
    //console.log(message);
    var key = message.key;

    async.waterfall([
        function getDeltas(callback) {
            stateBucket.get(key, callback);
        },
        function processDeltas(result, callback) {
            var deltas = result.value.deltas;

            var newItems = [];
            var modifiedItems = {};
            var deletedItems = {};

            for (var i in deltas) {
                if (deltas[i].op == "add")
                    newItems.push(deltas[i].value)
                else if (deltas[i].op == "replace" || deltas[i].op == "increment") {
                    modifiedItems[deltas[i].path] = deltas[i].value;
                } else {
                    var pathParts = deltas[i].path.split('/');

                    deletedItems[deltas[i].path] = true;
                }
            }


        }
    ], function(err, results) {

    });
}

module.exports = Write;