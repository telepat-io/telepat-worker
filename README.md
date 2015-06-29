# Telepat Workers

There are 3 main workers:

* Aggregator: writes deltas (individual operation on the database) in the database
* Writer: reads deltas and processes them (CRUD operations)
* Track: *not implemented yet*

Then there are the client transport workers. These workers are in charge of communicating with the client. They send
messages containing changes to the database objects. Right now there are 3 transport workers: for Android (using gcm),
iOS (APN) and sockets for other clients.

## Quick start up guide

`node index.js -t <topic name> -i <worker index>`

* **topic name** is the name of the kafka topic (the type of worker): aggregation, write, track, etc.
* **worker index** this is necessary because you can start multiple workers on the same topic

## Configuring

There are two ways to configure: either modify the config file `./config.json` or
set up environment variables (this method is the most convenient):

* `TP_KFK_HOST`: Kafka (zooekeeper) server
* `TP_KFK_PORT`: Kafka (zooekeeper) server port
* `TP_KFK_CLIENT`: Name for the kafka client
* `TP_CB_HOST`: Couchbase server
* `TP_CB_BUCKET`: Main data bucket of the couchbase server
* `TP_CB_STATE_BUCKET`: State bucket of the couchbase server
* `TP_REDIS_HOST`: Redis database server
* `TP_REDIS_PORT`: Redis server port
