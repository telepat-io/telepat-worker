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
