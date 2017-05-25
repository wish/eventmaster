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
	event_id UUID,
	parent_event_id UUID,
	dc text,
	topic_name text,
	host text,
	target_host_set set<text>,
	user text,
	event_time timestamp,
	tag_set set<text>,
	data_json text,
	event_type int,
	PRIMARY KEY ((event_id,topic_name),event_time)
);

// Create table to store distinct topics
CREATE TABLE IF NOT EXISTS event_topic (
	id int,
	topic_name text,
	PRIMARY KEY (topic_name)
);

// Create table to store distinct dcs
CREATE TABLE IF NOT EXISTS event_dc (
	id int,
	dc text,
	PRIMARY KEY (dc)
);