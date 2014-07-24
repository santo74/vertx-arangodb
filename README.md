[![Build Status](https://drone.io/github.com/santo74/vertx-arangodb/status.png)](https://drone.io/github.com/santo74/vertx-arangodb/latest)

[ ![Download](https://api.bintray.com/packages/santo/vertx-mods/vertx-arangodb/images/download.png) ](https://bintray.com/santo/vertx-mods/vertx-arangodb/_latestVersion)

vertx-arangodb
==============

This is a Non-blocking [Vert.x](http://vertx.io) persistor module for the [ArangoDB](http://www.arangodb.org) Multi-model database using its [REST](http://www.arangodb.org/manuals/current/ImplementorManual.html) interface.

## Requirements

* Vert.x 2.0+
* A working ArangoDB 2.0+ server/cluster, either on the local network or external.   
**Note**: The new Graph API (GharialAPI) requires ArangoDB 2.2+


## Installation

There are several ways to install the module:

* define the module in you application's config file and Vert.x will download the module for you once you start you application for the first time:


        "persistorConfig": {
            "type": "module",
            "name": "santo~vertx-arangodb~1.1.0",
            "enabled": true,
            "instances": 1,
            ...
        }


* install manually on the command line

`vertx install santo~vertx-arangodb~1.1.0`

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
* `generic` : If true, the generic API will be enabled. If false, the generic API won't be available. Defaults to `true`.

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
This section provides an overview of the API types and actions that are currently supported by the module, displayed in the following format:
* `type 1`
    * `action A`
    * `action B` *`(action C)`*
* `type 2`
    * `action A`
    * `action B`

Where type corresponds to a particular ArangoDB API and action to an action that can be executed in the context of that API.
(Actions between brackets are aliases that can be used instead, e.g. action C is an alias for action B in the type 1 API of the example above.)

* `database` : manage databases
    * `current` : retrieve information about current database
    * `user` : retrieve a list of databases the current user can access
    * `list` : retrieve a list of all existing databases
    * `create` : create a new database
    * `drop` : drop an existing database
* `document` : manage documents
    * `read` *`(get)`* *`(by-id)`* : read a document
    * `create` : create a new document
    * `replace` : replace a document
    * `patch` *`(update)`* : patch a document
    * `delete` : delete a document
    * `head` *`(header)`* : read a document header
    * `list` : list all documents in a collection
* `edge` : manage node relations
    * `read` *`(get)`* *`(by-id)`* : read an edge
    * `create` : create a new edge
    * `replace` : replace an edge
    * `patch` *`(update)`* : patch an edge
    * `delete` : delete an edge
    * `head` *`(header)`* : read an edge header
    * `relations` : read in- and/or outbound edges for a node
* `aql` : validate or execute AQL queries
    * `execute` *`(cursor)`* *`(execute-cursor)`* : execute an AQL query and create a cursor if applicable
    * `validate` *`(validate-query)`* : parse an AQL query and validate it
    * `next` *`(cursor-next)`* *`(execute-next)`* : read next batch from cursor
    * `delete` *`(delete-cursor)`* : delete a cursor
    * `explain` *`(explain-query)`* : explains how an AQL query would be executed
    * `create-function` : create or replace an AQL user function
    * `get-function` : retrieve all registered AQL user functions, optionally for given namespace
    * `delete-function` : delete the specified AQL user function or all user functions in namespace
* `query` : perform simple queries
    * `all` : retrieve all documents from a collection
    * `by-example` : find documents matching the provided example
    * `first-example` : find first document matching the provided example
    * `by-example-hash` : find all documents matching the provided example, using a specified hash index
    * `by-example-skiplist` : find all documents matching the provided example, using a specified skiplist index
    * `by-example-bitarray` : find all documents matching the provided example, using a specified bitarray index
    * `by-condition-skiplist` : find all documents matching the provided condition, using a specified skiplist index
    * `by-condition-bitarray` : find all documents matching the provided condition, using a specified bitarray index
    * `any` : retrieve a random document from a collection
    * `range` : find documents within a given range. (requires a skiplist index)
    * `near` : find documents near given coordinates. (requires a geo index)
    * `within` : find documents within a given radius around specified longitude and latitude coordinates
    * `fulltext` : find documents in a collection matching the specified fulltext query. (requires a fulltext index)
    * `remove-by-example` : remove all documents matching the provided example
    * `replace-by-example` : replace all documents matching the provided example with the provided replacement document
    * `update-by-example` : update attributes in documents matching the provided example with the provided updated attributes 
    * `first` : return the first document of a collection (ordered by insertion/update time)
    * `last` : return the last document of a collection (ordered by insertion/update time)
* `collection` : manage collections
    * `create` : create a new collection
    * `delete` : delete an existing collection
    * `truncate` : remove all documents from a collection (leave indexes intact)
    * `load` : load a collection into memory
    * `unload` : unload a collection from memory
    * `rename` : rename a collection
    * `rotate` : rotate the journal of a collection
    * `get-properties` : read properties of a collection
    * `change-properties` : change properties of a collection
    * `get` *`(read)`* *`(list)`* : get basic information about a specific collection or all collections (id, name, status, type)
    * `count` : get the number of documents in a collection
    * `figures` : get statistics for a collection
    * `revision` : get the revision id of a collection
    * `checksum` : calculate a checksum of a collection
* `index` : manage indexes
    * `read` *`(get)`* *`(list)`* : get information about a specific index or all indexes of a collection
    * `create` : create a new index
    * `delete` : delete an existing index
* `transaction` : execute a transaction on the server
    * `execute` : execute a transaction
* `graph` : manage graphs with exactly 1 edge collection and 1 vertex collection   
***(deprecated since ArangoDB 2.2 / vertx-arangodb 1.1)***
    * `create` *`(create-graph)`* : create a graph
    * `read` *`(get)`* *`(read-graph)`* *`(get-graph)`* : get properties of a specific graph or all graphs
    * `delete` *`(delete-graph)`* : delete an existing graph including all related edges and vertices
    * `create-vertex` : create a new vertex in the specified graph
    * `read-vertex` *`(get-vertex)`* : get properties of a specific vertex in the specified graph
    * `replace-vertex` : replace the properties of the specified vertex
    * `update-vertex` : partially updates the properties of the specified vertex
    * `delete-vertex` : deletes the specified vertex
    * `get-vertices` : gets an optionally filtered list of related vertices of the specified start vertex
    * `create-edge` : create a new edge in the specified graph
    * `read-edge` *`(get-edge)`* : get properties of a specific edge in the specified graph
    * `replace-edge` : replace the properties of the specified edge
    * `update-edge` : partially updates the properties of the specified edge
    * `delete-edge` : deletes the specified edge
    * `get-edges` : gets an optionally filtered list of edges for which the specified start vertex is an inbound or outbound relation
* `gharial` : manage graphs with one or more edge and/or vertex collections
    * `create` *`(create-graph)`* : create a graph
    * `read` *`(get)`* *`(read-graph)`* *`(get-graph)`* : get properties of a specific graph or all graphs
    * `delete` *`(drop)`* *`(delete-graph)`* *`(drop-graph)`* : delete an existing graph including all related edges and vertices
    * `list-vertex-collections` : Lists all vertex collections within a graph
    * `add-vertex-collection` : add a vertex collection to the set of collections of a graph
    * `remove-vertex-collection` : remove a vertex collection from the graph (and optionally delete the collection if not used in other graphs)
    * `list-edge-collections` : Lists all edge collections within a graph
    * `add-edge-collection` : add an edge definition to a graph
    * `replace-edge-collection` : change a specific edge definition in all graphs of the database
    * `remove-edge-collection` : remove an edge definition from a graph (and optionally delete the collection as well if not used in other graphs)
    * `create-vertex` : create a new vertex in the specified graph
    * `read-vertex` *`(get-vertex)`* : get data of a specific vertex in the specified graph
    * `update-vertex` *`(modify-vertex)`* : update the data of the specified vertex
    * `replace-vertex` : replace the data of the specified vertex
    * `delete-vertex` *`(remove-vertex)`* : remove the specified vertex from the collection
    * `create-edge` : create a new edge in the specified graph
    * `read-edge` *`(get-edge)`* : get data of a specific edge in the specified graph
    * `update-edge` *`(modify-edge)`* : update the data of the specified edge
    * `replace-edge` : replace the data of the specified edge
    * `delete-edge` *`(remove-edge)`* : deletes the specified edge
* `traversal` : walk over a graph stored in one edge collection
    * `traverse` : traverse a graph, starting from the given vertex and following edges in the specified edge collection
* `generic` : this custom API allows to perform plain HTTP requests, e.g. to perform an action that is not (yet) implemented in the module. This API should be used with caution and can be disabled via the config file.
    * `GET` : performs an HTTP GET request on the specified path
    * `POST` : performs an HTTP POST request on the specified path
    * `PUT` : performs an HTTP PUT request on the specified path
    * `PATCH` : performs an HTTP PATCH request on the specified path
    * `DELETE` : performs an HTTP DELETE request on the specified path
    * `HEAD` : performs an HTTP HEAD request on the specified path

Please also take a look at the integration tests where you can find examples of all the different types and actions supported by the module.

### Receiving a response

The response will differ per request type/action, but always contains the fields "status", "statuscode", "message", "severity" and "result", where:

* `status` : "ok" if the request was successful, "error" if the request was not successful.
* `statuscode` : contains the HTTP status code of the REST request to the ArangoDB instance. (Added because we don't always receive a return code in the response object of ArangoDB)
* `message` : contains a short message about the result, most of the time either "success" or "error", but can also be more specific such as "invalid type specified (sharding)".
* `severity` : The severity of the result. Can be "success", "info", "warning" or "danger".
* `result` : a Json object or array (depending on the request type/action) containing the ArangoDB response

Example of a successful request:

    {
        "status": "ok",
        "statuscode": 200,
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
        "statuscode": 404,
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

The module currently only supports a subset of the functionality provided by ArangoDB's REST API.
While most of the operational actions are available, other action types such as administration related ones are not implemented (yet).
If you really need them now, you can try to use the generic API instead.

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
