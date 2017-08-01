// Create keyspace		
CREATE KEYSPACE IF NOT EXISTS event_master		
WITH REPLICATION = {
	'class' : 'SimpleStrategy',
	'replication_factor': 1
};

// Use keyspace
USE event_master;

// Create event_logs table
CREATE TABLE IF NOT EXISTS event (
	event_id text,
	parent_event_id text,
	dc_id UUID,
	topic_id UUID,
	host text,
	target_host_set set<text>,
	user text,
	event_time timestamp,
	tag_set set<text>,
	data_json text,
	received_time timestamp,
	PRIMARY KEY ((dc_id,topic_id),event_id)
)
WITH CLUSTERING ORDER BY (event_id DESC);

// Create table to store events to flush to ES
CREATE TABLE IF NOT EXISTS temp_event (
	event_id text,
	parent_event_id text,
	dc_id UUID,
	topic_id UUID,
	host text,
	target_host_set set<text>,
	user text,
	event_time timestamp,
	tag_set set<text>,
	data_json text,
	received_time timestamp,
	PRIMARY KEY ((dc_id,topic_id),event_id)
)
WITH CLUSTERING ORDER BY (event_id DESC);

// Create table to store distinct topics
CREATE TABLE IF NOT EXISTS event_topic (
	topic_id UUID,
	topic_name text,
	data_schema text,
	PRIMARY KEY (topic_id)
);

// Create table to store distinct dcs
CREATE TABLE IF NOT EXISTS event_dc (
	dc_id UUID,
	dc text,
	PRIMARY KEY (dc_id)
);