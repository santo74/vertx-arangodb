[![Build Status](https://drone.io/github.com/santo74/vertx-arangodb/status.png)](https://drone.io/github.com/santo74/vertx-arangodb/latest)

[ ![Download](https://api.bintray.com/packages/santo/vertx-mods/vertx-arangodb/images/download.png) ](https://bintray.com/santo/vertx-mods/vertx-arangodb/_latestVersion)

vertx-arangodb
==============

This is a Non-blocking [Vert.x](http://vertx.io) persistor module for the [ArangoDB](http://www.arangodb.org) Multi-model database using its [REST](http://www.arangodb.org/manuals/current/ImplementorManual.html) interface.

## Requirements

* Vert.x 2.0+
* A working ArangoDB server/cluster, either on the local network or external.

## Installation

There are several ways to install the module:

* define the module in you application's config file and Vert.x will download the module for you once you start you application for the first time:


        "persistorConfig": {
            "type": "module",
            "name": "santo~vertx-arangodb~1.0.0-beta1",
            "enabled": true,
            "instances": 1,
            ...
        }


* install manually on the command line

`vertx install santo~vertx-arangodb~1.0.0-beta1`

* download manually from bintray. See download link at the top of this document

## Configuration

    {
        "address": "santo.vertx.arangodb.rest",
        "dbname": "testdb",
        "host": "localhost",
        "port": 8529,
        "username": "admin",
        "password": "adminpwd",
        "ssl": false,
        "ssl_trustall": true,
        "ssl_verifyhost": false,
        "truststore": "truststore.jks",
        "truststore_password": "pwd",
        "keystore": "keystore.jks",
        "keystore_password": "pwd",
        "maxpoolsize": 50,
        "keepalive": false,
        "compression": false,
        "connect_timeout": 60000,
        "reuse_address": false,
        "tcp_keepalive": false,
        "tcp_nodelay": true
    }

### Basic options
* `address` : The address on which the module should listen on the eventbus. Defaults to `santo.vertx.arangodb`.
* `dbname` : The name of the database you want to use. Defaults to `testdb`.
* `host` : The hostname of the ArangoDB server. Defaults to `localhost`.
* `port` : The port on which ArangoDB is listening. Defaults to `8529`.
* `username` : Username for connecting to ArangoDB if authentication is enabled. Defaults to no username (i.e. no authentication required).
* `password` : Password for connecting to ArangoDB if authentication is enabled. Defaults to no password (i.e. no authentication required).
* `ssl` : If true, then a secure SSL connection will be used towards the database. If false, then an unsecure connection is used. Defaults to `false`.
* `ssl_trustall` : If true, then all certificates are accepted. If false, then the certificate should match a certificate from the truststore. Defaults to false.
* `ssl_verifyhost` : If true, then validate the database's certificate hostname against the specified hostname. Defaults to true.
* `truststore` : Path to the SSL truststore. Only required if SSL is enabled and ssl_trustall is disabled. Defaults to no truststore.
* `truststore_password` : The password that is required to access the truststore. Defaults to no password.
* `keystore` : Path to the SSL keystore which contains the client certificate in case client authentication is required on the server. Defaults to no keystore.
* `keystore_password` : The password that is required to access the keystore. Defaults to no password.

### Tuning options
* `maxpoolsize` : The maximum number of connections to maintain in the connection pool. Defaults to `10`.
* `keepalive` : If true, then the connection will be returned to the pool after the request has ended rather than being closed. Please note however that this implicitly enables pipelining as well. If false, a new connection will be created for each request. Defaults to `false`.
* `compression` : If true, it will indicate to the server that we support compressed response bodies. Defaults to `false`.
* `connect_timeout` : Connection timeout in milliseconds. Defaults to `15000` (15 sec).
* `reuse_address` : If true, then addresses in TIME_WAIT state can be reused after they have been closed. Defaults to `false`.
* `tcp_keepalive` : If true, then tcp keep alive is enabled. If false, tcp keep alive is disabled. Defaults to `false`.
* `tcp_nodelay` : If true, then Nagle's Algorithm is disabled. If false, then it's enabled. Defaults to `true`.

## Usage

### Sending a request

The content of the request is different for each action, but in all cases the module expects a json message
with at least a **type** and **action** parameter, e.g.:

    {
        "type": "database",
        "action": "list"
    }

#### supported types and actions

To be provided.

For now, please take a look at the integration tests where you can find examples of all the different types and actions supported by the module.

### Receiving a response

The response will differ per request type/action, but always contains the fields "status", "message", "severity" and "result", where:

* `status` : "ok" if the request was successful, "error" if the request was not successful.
* `message` : contains a short message about the result, most of the time either "success" or "error", but can also be more specific such as "invalid type specified (sharding)".
* `severity` : The severity of the result. Can be "success", "info", "warning" or "danger".
* `result` : a Json object or array (depending on the request type/action) containing the ArangoDB response

Example of a successful request:

    {
        "status": "ok",
        "message": "success",
        "severity": "success",
        "result": {
            "result": [
                "_system",
                "testdb"
            ],
            "error": false,
            "code": 200
        }
    }

Example of an unsuccessful request:

    {
        "status": "error",
        "message": "error",
        "severity": "danger",
        "result": {
            "error": true,
            "code": 404,
            "errorNum": 1228,
            "errorMessage": "database not found"
        }
    }

## Limitations

The module (currently) only supports a subset of the functionality provided by ArangoDB's REST API.
While most of the operational actions are available, other action types such as administration related ones are not implemented (yet).
Here is a list of what's missing:

* Aql User Functions
* Replication
* Bulk Imports
* Batch requests
* Administration and Monitoring
* User Management
* Async Results Management
* Endpoints
* Sharding
* Miscellaneous functions

