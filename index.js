var args = require('electron').argv();
var async = require('async');
var redis = require('redis');
require('colors');

var tlib = require('telepat-models');

var workerType = args.params.t;
var workerIndex = args.params.i;
/**
 *
 * @type {Base_Worker}
 */
var theWorker = null;

async.series([
	seriesCallback => {
		let extraObj = {
			name: workerType  + '-' + workerIndex,
			broadcast: workerType === 'sockets_transport'? true:false, 
			exclusive: false,
			type:  workerType
		}; 
		tlib.init(extraObj, seriesCallback, workerType);
	},
	(seriesCallback) => {	
		switch (workerType) {
			case 'aggregation': {
				var AggregationWorker = require('./lib/aggregation_worker');

				theWorker = new AggregationWorker(workerIndex, tlib.config);

				break;
			}
			case 'write': {
				var WriterWorker = require('./lib/writer_worker');

				theWorker = new WriterWorker(workerIndex, tlib.config);
				theWorker.redisClient = tlib.services.redisClient;
				theWorker.redisCacheClient = tlib.services.redisCacheClient;

				break;
			}
			case 'transport_manager': {
				var TransportManagerWorker = require('./lib/transport_manager');

				theWorker = new TransportManagerWorker(workerIndex, tlib.config);

				break;
			}
			default: {
				var workerTypeParts = workerType.split('_');
				
				if (workerTypeParts[1] === 'transport') {
					var ClientTransportWorker = require('./lib/client_transport/' + workerTypeParts[0]);

					theWorker = new ClientTransportWorker(workerIndex, tlib.config);
				} else {
					console.log('Invalid worker type "' + workerType + '"');
					process.exit(1);
				}
			}
		}
		seriesCallback();

	},
], function (err) {
	if (err) {
		throw err;
	}

	theWorker.ready();
});