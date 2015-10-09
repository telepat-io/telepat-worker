function importTest(name, path) {
	describe(name, function () {
		require(path);
	});
}

describe('Worker', function () {

	before(function (done) {
		this.timeout(15000);

		process.argv[3] = "-t";

		process.argv[5] = "-i";
		process.argv[6] = "0";

		var workerTypes = [ 'aggregation', 'write', 'track', 'android_transport', 'ios_transport', 'sockets_transport' ];

		process.argv[4] = workerTypes[0];
		aggregation = require('../index');
		process.argv[4] = workerTypes[1];
		write = require('../index');
		process.argv[4] = workerTypes[2];
		track = require('../index');
		process.argv[4] = workerTypes[3];
		android_transport = require('../index');
		process.argv[4] = workerTypes[4];
		ios_transport = require('../index');
		process.argv[4] = workerTypes[5];
		sockets_transport = require('../index');

			done();

	});



	importTest("1.Admin", './admin/admin');
	importTest("2.Context", './context/context');
	importTest("3.Device", './device/device');
	importTest("4.Object", './object/object');
	importTest("5.User", './user/user');
});
