var Base_Worker = require('./base_worker');
var Models = require('telepat-models');
var async = require('async');

var UpdateFriendsWorker = function(index) {
	Base_Worker.call(this, 'update_friends', index);
};

UpdateFriendsWorker.prototype = new Base_Worker();

UpdateFriendsWorker.prototype.processMessage = function(message) {
	var friendIds = message.friends;
	var baseKey = 'blg:'+Models.User._model.namespace+':fid:';

	if (friendIds.length > 0) {
		async.waterfall([
			function(callback) {
				async.map(friendIds, function(item, c) {
					c(null, baseKey+item);
				}, function(err, result) {
					Models.Application.bucket.getMulti(result, callback);
				});
			},
			function(userIds, callback) {
				async.each(Object.keys(userIds), function(k, callback1) {
					Models.User.update(userIds[k].value, [{op: 'append', path: 'friends', value: message.fid}], callback1);
				}, callback);
			}
		], function(err, result){
			if (err) {
				console.log(err);
				console.trace('Update friends trace.');
			}
		});
	}
};

module.exports = UpdateFriendsWorker;
