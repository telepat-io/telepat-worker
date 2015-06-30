var colors = require('colors');
var redis = require('redis');

redisClient = null;
config = {};

config.redis = require('./config.json').redis;

(function RedisClient(callback) {
	if (redisClient)
		redisClient = null;

	redisClient = redis.createClient(config.redis.port, config.redis.host);
	redisClient.on('error', function(err) {
		console.log('Failed'.bold.red+' connecting to Redis "'+config.redis.host+'": '+err.message);
		console.log('Retrying...');
		/*setTimeout(function () {
			RedisClient(callback);
		}, 1000);*/
	});
	redisClient.on('ready', function() {
		console.log('Client connected to Redis.'.green);
		callback();
	});
})(function() {
	redisClient.get('doesnt exist', function(err, result) {
		console.log(err, result);
	});

	/*redisClient.rpush(['blg:1:events', '{"op":"replace","path":"events/111/text","value":"puuusca si cureaua","guid":"f90e8826-3f85-4217-82f3-8425cec0f69d","ts":"83145743157240"}', '{"op":"replace","path":"vents/111/text","value":"puuusca si cureaua ","guid":"b57d4277-5e79-483e-adfb-b58f5a1654e5","ts":"8314648399940"}'], function(err, result) {
		if (err) return console.log(err);

		/!*console.log(result);
		redisClient.multi().
			lrange(['blg:1:events', 0, -1]).
			del('blg:1:events').
			exec(function(err, replies) {
				if (err) return console.log(err);

				/!*replies[0] = '['+replies[0]+']';

				console.log(JSON.parse(replies[0]));*!/
				console.log(replies[0]);

				redisClient.quit();
				process.exit(0);
			});*!/
	});*/
});

process.on('SIGINT', function() {
	redisClient.quit();
	process.exit(0);
});

process.on('uncaughtException', function() {
	redisClient.quit();
	process.exit(0);
});
