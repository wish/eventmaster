## Add Events
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

## Query Events
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

## Add Topic
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

## Update Topic
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

## Delete Topic
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

## Get Topics
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

## Add Data Center
```
POST /v1/dc
```
Example Request:
```
POST /v1/dc
Accept: application/json
Content-Type: application/json
{
	"DC_name": "dc1"
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

## Update Data Center
```
PUT /v1/dc/:name
```
Example Request:
```
PUT /v1/dc/dc1
Accept: application/json
Content-Type: application/json
{
	"DC_name": "dc3"
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


## Get Data Centers
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
The gRPC API supports all methods supported by the REST API. Refer to the [protobuf file](https://github.com/ContextLogic/eventmaster/blob/master/proto/eventmaster.proto) for details on usage.

## Rsyslog Server
Eventmaster facilitates centralized logging by translating logs into events and adding them to the event store.
To run Eventmaster's Rsyslog server, include the `-r` option:
```
$ eventmaster -r --rsyslog_port=50053 <other_options>
```

Rsyslog clients can be configured to send logs to Eventmaster's Rsyslog server over TCP by formatting logs according to the template found in the [sample Rsyslog client configuration template file](https://github.com/ContextLogic/eventmaster/blob/master/rsyslog-eventmaster.conf.erb).

If logs are encrypted with TLS, the `--ca_file`, `--cert_file`, and `--key_file` options must be specified to decrypt incoming messages.
