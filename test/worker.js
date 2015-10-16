function importTest(name, path) {

	describe(name, function () {
		require(path);
	});
}

function containsString (substring, string) {
	//console.log(substring);
	if (string)
	if (string.indexOf(substring) > -1) {
		//console.log(string);
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

		this.timeout(15000);

		process.argv[3] = "-t";

		process.argv[5] = "-i";
		process.argv[6] = "0";

		var workerTypes = [ 'aggregation', 'write', 'android_transport', 'ios_transport', 'sockets_transport' ];

		process.argv[4] = workerTypes[0];
		aggregation = require('../index');

		setTimeout(function(){},3000);

		deleteRequireCache('worker/index.js',require.cache);
		process.argv[4] = workerTypes[1];
		write = require('../index');

		/*deleteRequireCache('worker/index.js',require.cache);
		process.argv[4] = workerTypes[2];
		track = require('../index');

		deleteRequireCache('worker/index.js',require.cache);
		process.argv[4] = workerTypes[3];
		android_transport = require('../index');

		deleteRequireCache('worker/index.js',require.cache);
		process.argv[4] = workerTypes[4];
		ios_transport = require('../index');

		deleteRequireCache('worker/index.js',require.cache);
		process.argv[4] = workerTypes[5];
		sockets_transport = require('../index');*/

		setTimeout(done,3000);
	});

	importTest("1.Admin", './admin/admin');
	importTest("2.Context", './context/context');
	importTest("3.Device", './device/device');
	importTest("4.Object", './object/object');
	importTest("5.User", './user/user');
});
