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
	received_time timestamp,
	date text,
	PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS event_metadata (
	event_id text,
	data_json text,
	PRIMARY KEY (event_id)
);

// Tables to help with querying by column
CREATE TABLE IF NOT EXISTS event_by_topic (
	event_id text,
	topic_id UUID,
	event_time timestamp,
	date text,
	PRIMARY KEY ((topic_id, date), event_time))
WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE IF NOT EXISTS event_by_dc (
	event_id text,
	dc_id UUID,
	event_time timestamp,
	date text,
	PRIMARY KEY ((dc_id, date), event_time))
WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE IF NOT EXISTS event_by_host (
	event_id text,
	host text,
	event_time timestamp,
	date text,
	PRIMARY KEY ((host, date), event_time))
WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE IF NOT EXISTS event_by_user (
	event_id text,
	user text,
	event_time timestamp,
	date text,
	PRIMARY KEY ((user, date), event_time))
WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE IF NOT EXISTS event_by_parent_event_id (
	event_id text,
	parent_event_id text,
	event_time timestamp,
	date text,
	PRIMARY KEY ((parent_event_id, date), event_time))
WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE IF NOT EXISTS event_by_date (
	event_id text,
	event_time timestamp,
	date text,
	PRIMARY KEY (date, event_time))
WITH CLUSTERING ORDER BY (event_time DESC);

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