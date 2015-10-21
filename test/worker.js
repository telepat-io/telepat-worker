var common = require('./common.js');
var async = require('async');
var DELAY = common.DELAY;

function importTest(name, path) {

	describe(name, function () {
		require(path);
	});
}

function containsString (substring, string) {

	if (string && string.indexOf(substring) > -1) {
		return string;
	}
}

function deleteRequireCache (toDelete, cache){
	var indexPath;

	for(var k in cache)
	{
		indexPath = containsString(toDelete,k);
		if(indexPath !== undefined)
			break;
	}

	delete require.cache[indexPath];
}

describe('Worker', function () {

	before(function (done) {

		this.timeout(80000);

		process.argv[3] = "-t";

		process.argv[5] = "-i";
		process.argv[6] = "0";

		var workerTypes = [ 'aggregation', 'write', 'update_friends', 'android_transport', 'ios_transport', 'sockets_transport' ];

		async.series([
			function(callback){
				process.argv[4] = workerTypes[0];
				var aggregation = require('../index');
				setTimeout(callback,3000);
			},
			function(callback){
				deleteRequireCache('worker/index.js',require.cache);
				process.argv[4] = workerTypes[1];
				var write = require('../index');
				setTimeout(callback,1000);
			},
			function(callback){
				deleteRequireCache('worker/index.js',require.cache);
				process.argv[4] = workerTypes[2];
				var update_friends = require('../index');
				setTimeout(callback,3000);
			},
			function(callback){
				deleteRequireCache('worker/index.js',require.cache);
				process.argv[4] = workerTypes[3];
				var android_transport = require('../index');
				setTimeout(callback,3000);
			},
			function(callback){
				deleteRequireCache('worker/index.js',require.cache);
				process.argv[4] = workerTypes[4];
				var ios_transport = require('../index');
				setTimeout(callback,3000);
			},
			function(callback){
				deleteRequireCache('worker/index.js',require.cache);
				process.argv[4] = workerTypes[5];
				process.env.TP_SCKT_PORT = 8080;
				var sockets_transport = require('../index');
				setTimeout(function(){
					callback();
					done();
				},6000);
			}]);
	});

	describe('Routes', function () {
		importTest("1.Admin", './admin/admin');
		importTest("2.Context", './context/context');
		importTest("3.Device", './device/device');
		importTest("4.Object", './object/object');
		importTest("5.User", './user/user');
	});

/*	describe('Unit Testing', function () {
		importTest("1.Base Worker", './unit/base_worker');
	});*/
});
