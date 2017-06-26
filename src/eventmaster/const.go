package main

const (
	EventTypeMapping = `
		{
			"properties": {
				"event_id": { 
					"type": "text",
					"fields" : {
						"keyword" : {
							"type" : "keyword",
							"ignore_above" : 256
						}
					}
				},
				"parent_event_id": { 
					"type": "text",
					"fields" : {
						"keyword" : {
							"type" : "keyword",
							"ignore_above" : 256
						}
					}
				},
				"event_time": {
					"type": "date"
				},
				"dc_id": { 
					"type": "text",
					"fields" : {
						"keyword" : {
							"type" : "keyword",
							"ignore_above" : 256
						}
					}
				},
				"topic_id": { 
					"type": "text",
					"fields" : {
						"keyword" : {
							"type" : "keyword",
							"ignore_above" : 256
						}
					}
				},
				"host": { 
					"type": "text",
					"fields" : {
						"keyword" : {
							"type" : "keyword",
							"ignore_above" : 256
						}
					}
				},
				"target_host_set": {
					"type": "text",
					"fields" : {
						"keyword" : {
							"type" : "keyword",
							"ignore_above" : 256
						}
					}
				},
				"user": {
					"type": "text",
					"fields" : {
						"keyword" : {
							"type" : "keyword",
							"ignore_above" : 256
						}
					}
				},
				"tag_set": {
					"type": "text",
					"fields" : {
						"keyword" : {
							"type" : "keyword",
							"ignore_above" : 256
						}
					}
				}
			}
		}`
)
