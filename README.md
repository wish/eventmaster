# eventmaster

There are lots of events that happen in production such as deploys, bounces, scale-up, etc. Right now these events happen, but we have no centralized place to track them -- which means we have no centralized place to search them. This project includes:
- Creating an event store
- API for sending/firing events
- Potential integrations into other services for searching etc. (grafana annotations)
- Working with other pieces of infra to expose meaningful events

## Setup
### Dependencies
- Go v1.8.x
- Cassandra v3.10

### Get Code
```
$ mkdir $GOPATH/src/github.com/ContextLogic
$ cd $GOPATH/src/github.com/ContextLogic/
$ git clone git@github.com:ContextLogic/eventmaster.git
```

### Get Required Packages
Glide is used to manage project dependencies: https://glide.sh/.
It can be installed by running
```
$ curl https://glide.sh/get | sh
```

To install required packages, run
```
$ cd $GOPATH/src/github.com/ContextLogic/eventmaster
$ glide install
```

### Configuration
Modify `em_config.json` to specify the address of Cassandra along with other database options. Alternatively, [service lookup](https://github.com/ContextLogic/goServiceLookup) can be used to find the IPs of Cassandra clusters by specifying the `--cassandra_servicename` command line options.

### Database Setup
Execute `schema.cql` on your Cassandra cluster. This will set up the `event_master` keyspace and `event`, `temp_event`, `event_topic`, and `event_dc` tables. Note that the `class` and `replication_factor` can be modified as long as the `consistency` attribute is configured accordingly in the `em_config.json`.
```
$ cqlsh -f schema.cql
```

### Running
To use options set in `em_config.json` or defaults, run
```
$ make run
```
To use service lookup, run
```
$ go run src/eventmaster/*.go --cassandra_servicename=<cassandra-servicename> --cassandra_port=<cassandra-port>
```
The port of the eventmaster server can be configured using the `--port` option.

Open the eventmaster UI in your browser (default: `http://localhost:50052`). Through the UI, events, topics and data centers can be added and events can be queried.

### Tests
Tests can be run using
```
$ make run
```

## REST API
### Add Events
```
POST /v1/event
```
Example Request:
```
POST /v1/event
Accept: application/json
Content-Type: application/json

{
	"parent_event_id": "a252eee1-2ec4-4010-8213-332317ccdf30",
	"event_time": 1497309509,
	"dc": "dc1",
	"topic_name": "security",
	"tag_set": ["login","user_login"],
	"host": "host1",
	"target_host_set": ["host2","host3"],
	"user": "someone",
	"data": {"first_name":"admin", "user_id":12345}
}
```
Required Fields: `dc`, `topic_name`, and `host`
The `topic_name` and `dc` fields must have already been added (See [Add Topic](#add-topic) and [Add Dc](#add-data-center)).

Example Response:
```
HTTP/1.1 200
Content-Type: application/json

{
	"event_id": 5280a88d-b533-4695-8090-9dcef8d35455
}
```

### Query Events
```
GET /v1/event
```
Example Request:
```
GET /v1/event?dc="dc1"&dc="dc2"&host="host1"&data={"user_id":123}
Accept: application/json
Content-Type: application/json
```
Accepted query parameters: `parent_event_id`, `event_time`, `dc`, `topic_name`, `tag_set`, `host`, `target_host_set`, `user`, `data`

Example Response:
```
HTTP/1.1 200
Content-Type: application/json

{
	"results": [{
	   	 "event_id": "afbd0e88-7383-4866-b058-dfcc3ac450fd",
	   	 "parent_event_id": "",
	   	 "event_time": 1497309509,
	   	 "dc": "dc1",
	   	 "topic_name": "security",
	   	 "tag_set": ["tag1"],
	   	 "host": "host1",
	   	 "target_host_set": ["host2"],
	   	 "user": "",
	   	 "data": "{"user_id":123}",
    }, {
	   	 "event_id": "2dcf0edf-43c3-4839-82d8-d816e8b31d5a",
	   	 "parent_event_id": "",
	   	 "event_time": 1497309509,
	   	 "dc": "dc2",
	   	 "topic_name": "security",
	   	 "tag_set": ["tag1", "tag2"],
	   	 "host": "host1",
	   	 "target_host_set": null,
	   	 "user": "",
	   	 "data": "{"user_id":123}",
    }]
}
```

### Add Topic
```
POST /v1/topic
```
Example Request:
```
POST /v1/topic
Accept: application/json
Content-Type: application/json

{
	"topic_name": "security",
	"data_schema": {
	    "title": "Security data",
	    "description": "Additional data for security events",
	    "type": "object",
	    "required": ["user_id", "first_name"],
	    "properties": {
	        "first_name": {
	            "type": "string",
	            "default": "admin"
	        },
	        "last_name": {
	            "type": "string"
	        },
	        "user_id": {
	            "type": "integer",
	            "minimum": 0
	        },
	    }
	}
}
```
Note: `data_schema` is optional and will default to '{}'. A sample data schema can be found [here](https://github.com/ContextLogic/eventmaster/blob/master/sample_data_schema.json).

Example Response:
```
HTTP/1.1 200
Content-Type: application/json

{
	"topic_id": "c8a81b3b-a581-4330-8443-7572d753cffe"
}
```

### Update Topic
```
PUT /v1/topic/:name
```
Example Request:
```
PUT /v1/topic/security
Accept: application/json
Content-Type: application/json

{
	"topic_name": "super-security",
	"data_schema": {}
}
```
Note: `topic_name` and `data_schema` are optional fields. `data_schema`, if specified, must be backwards compatible with the old schema (newly added required fields must have set defaults).

Example Response:
```
HTTP/1.1 200
Content-Type: application/json

{
	"topic_id": "c8a81b3b-a581-4330-8443-7572d753cffe"
}
```

### Delete Topic
```
DELETE /v1/topic/:name
```
Note: Once a topic is deleted, all associated events under the topic will be deleted in ES - i.e. they will not appear in query results.

Example Request:
```
DELETE /v1/topic/security
```

Example Response:
```
HTTP/1.1 200
```

### Get Topics
```
GET /v1/topic
```
Example Request:
```
GET /v1/topic
Accept: application/json
Content-Type: application/json
```

Example Response:
```
HTTP/1.1 200
Content-Type: application/json

{
	"results": [
		{
			"topic_name":"security", 
			"data_schema": {
			    "title": "Security data",
			    "description": "Additional data for security events",
			    "type": "object",
			    "required": ["user_id", "first_name"],
			    "properties": {
			        "first_name": {
			            "type": "string",
			            "default": "wendy"
			        },
			        "last_name": {
			            "type": "string"
			        },
			        "user_id": {
			            "type": "integer",
			            "minimum": 0
			        },
			    }
			}
		},
		{
			"topic_name":"test",
			"data_schema": {}
		}
	]
}
```

### Add Data Center
```
POST /v1/dc
```
Example Request:
```
POST /v1/dc
Accept: application/json
Content-Type: application/json
{
	"dc": "dc1"
}
```

Example Response:
```
HTTP/1.1 200
Content-Type: application/json

{
	"id": "fb9a0dd0-5d69-43c3-b4aa-0ef08698c580"
}
```

### Update Data Center
```
PUT /v1/dc/:name
```
Example Request:
```
PUT /v1/dc/dc1
Accept: application/json
Content-Type: application/json
{
	"dc": "dc3"
}
```

Example Response:
```
HTTP/1.1 200
Content-Type: application/json

{
	"id": "fb9a0dd0-5d69-43c3-b4aa-0ef08698c580"
}
```


### Get Data Centers
```
GET /v1/dc
```
Example Request:
```
GET /v1/dc
Accept: application/json
Content-Type: application/json
```

Example Response:
```
HTTP/1.1 200
Content-Type: application/json

{
	"results": ["dc1", "dc2"]
}
```

## gRPC API
The gRPC API supports all methods supported by the REST API. Refer to the [protobuf file](https://github.com/ContextLogic/eventmaster/blob/master/eventmaster/eventmaster.proto) for details on usage.

## Rsyslog Server
Eventmaster facilitates centralized logging by translating logs into events and adding them to the event store.
To run Eventmaster's Rsyslog server, include the `-r` option:
```
$ go run src/eventmaster/*.go -r --rsyslog_port=50053 <other_options>
```

Rsyslog clients can be configured to send logs to Eventmaster's Rsyslog server over TCP by formatting logs according to the template found in the [sample Rsyslog client configuration template file](https://github.com/ContextLogic/eventmaster/blob/master/rsyslog-eventmaster.conf.erb).

If logs are encrypted with TLS, the `--ca_file`, `--cert_file`, and `--key_file` options must be specified to decrypt incoming messages.
