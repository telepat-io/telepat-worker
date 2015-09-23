var Base_Worker = require('./base_worker');
var Models = require('telepat-models');
var async = require('async');

var UpdateFriendsWorker = function(index) {
	Base_Worker.call(this, 'update_friends', index);
};

UpdateFriendsWorker.prototype = Base_Worker.prototype;

UpdateFriendsWorker.prototype.processMessage = function(message) {
	var friendIds = message.friends;

	if (friendIds.length > 0) {
		async.waterfall([
			function(callback) {
				Models.Application.datasource.dataStorage.connection.search({
					index: Models.Application.datasource.dataStorage.config.index,
					type: 'user',
					body: {
						terms: {
							fid: friendIds,
							minimum_should_match: 1
						}
					}
				}, function(err, results) {
					if (err) return callback(err);
					callback(null, results);
				});
			},
			function(userResults, callback) {
				async.each(userResults.hits.hits, function(user, callback1) {
					Models.User.update(user.id, user.application_id, [{op: 'append', path: 'user/'+user.id+'/friends', value: message.fid}], callback1);
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
