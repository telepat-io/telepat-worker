# 0.4.2

* Author validation checks in aggregation worker
* GCM api key is now retrieved from the application object
* Added support for SystemMessages

# 0.4.1

* Added support for SSL on sockets client transport worker
* Bugfix: missing subscriptions array on notification payload

# 0.4.0

* Big performance tweaks, the whole workers had been reworked
* update_friends worker removed
* New worker: transport_manager which is in charge of dispatching
notifications to the right transport worker
* Write worker no longer takes all deltas, but instead initially fetches
1 delta then it doubles this limit everytime there are left in the redis
Key
* Android and IOS transport workers now send messages in smaller chunks
in order to avoid going over the payload limit
* iOS transport worker now fetches the certificates from the database
* iOS transport now supports sending classic push notifications when a
certain app model has been created
* Sockets transport worker now only has 1 event: bind_device where the
client must send a message in order to activate its subscriptions
* Device token for volatile transports are now being removed when the
client disconnects and re-added after reconnecting

# 0.3.0

* Renamed some fields on the sockets transport:
	* **deviceId** to **device_id**
* **bind_device** event on sockets transport is used to activate the
device. After disconnecting the devices is deactivated.
* Socket workers now have their own queue
* Write workers send notifications to the socket worker which has the
affected device client, instead of broadcasting
* Fixed a bug were items were created multiple times under high
concurency

# 0.2.8

* Bugfix: `processMessage` Fixed write worker hang on end of main async function

# 0.2.7

* Fixed bug which caused the writer to not create items
* Fixed bug where writer would hang on context create/update operations
* Operations that have the **instant** flag in the delta will not be persisted to database

# 0.2.6

* Fixed bugs
* Notifications are sent to all application devices when contexts are created/updated/removed
* Sockets transport listens to an event from the client to bind its device to the socket ID.
* Workers use TelepatLogger instead of console.log

# 0.2.5

* All applications are loaded on boot up
* Socket transport should now all receive 1 message from the writer (broadcast)

# 0.2.4

* Fixed update_friends worker
* Refactored messaging client: moved it to telepat-models

# 0.2.3

* Fixed A LOT of bugs
* Integrated the new ElasticSearch db adapter
* Better error handling when env variables are missing and/or config file is missing

# 0.2.2

* Various bug and crash fixes
* Added context id to operations on application models
* Update patches are received one by one (no longer an array of patches)
* Created objects are returned in the "new" deltas message (with full properties)
* Added this CHANGELOG

# 0.2.0

* Added dockerfile
* Major reworking and refactoring of the code:
	* Each type of worker has it's own class
	* Each type inherits a base worker
	* Easy to extend with new types of workers and new transport clients
	* Can use a different messaging framework as long as it's implemented
* Sockets transport client can be configured to use a different listening port (default 80)

# 0.1.3

* Added LICENSE and README files
* Environment variables for redis database config
* Fixed bugs

# 0.1.2

* Each update uses timestamp to preserve the order in which the updates appear to be sent

# 0.1.1

* Fixed sockets transport client

# 0.1.0

* Initial pre-alpha release
