# Telepat Workers

There are 4 main types of workers:

* Aggregators: writes deltas (deltas are a representation of an individual operations on the database) in the database.
These workers then send messages to writers notifying them that they have work to do. This worker consumes messages from
 the **aggregator** topic.
* Writers: reads deltas and processes them (CRUD operations). They then send messages to transport manager workers with the processed
deltas. This worker consumes messages from the **write** topic.
* Transport managers: gets all devices in the affected subscriptions from the deltas. Then it sorts them according to the transport
type each device uses and sends messages to the corresponding workers. This worker consumes messages from the **transport_manager**
topic.

Then there are the client transport type workers. These type of workers are in charge of communicating with the client.
They send messages containing changes to the database objects. Right now there are 3 transport workers: for Android (using gcm),
iOS (APN) and sockets for other clients. These workers send messages only to clients that are subscribed to the channel
(writers include these in the messages sent by them to transport workers).

* **android_transport**, the topic from where the android transport worker consumes messages
* **ios_transport**, the topic from where the ios transport worker consumes messages
* **sockets_transport**, the topic from where the sockets transport worker consumes messages

## Quick start up guide

`node index.js -t <topic name> -i <worker index>`

* **topic name** is the name of the kafka topic (the type of worker): aggregation, write, track, etc.
* **worker index** this is necessary because you can start multiple workers on the same topic

**IMPORTANT**: worker index is needed for volatile transports (server based, like sockets_transport),
that's because these have their own message queue to consume from

## Configuring

There are two ways to configure: either by using the `config.example.json` config file (rename it into config.json)
or by setting up environment variables (this method is the most convenient):

* `TP_MSG_QUE`: Name of the messaging client you want to use. Should be the same as the exported variable in
telepat-models
* `TP_MAIN_DB`: Name of the main database which to use. Should be the same as the exported variable in telepat-models

**Important**: You need to set up the other config variables specified in the `telepat-models` README file for resources
that you're using.
