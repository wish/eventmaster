// Create keyspace		
CREATE KEYSPACE IF NOT EXISTS event_master		
WITH REPLICATION = {
	'class' : 'SimpleStrategy',
	'replication_factor': 1
};

// Use keyspace
USE event_master;

// Create event_logs table
CREATE TABLE IF NOT EXISTS event_logs (
	date text,
	dc text,
	topic_name text,
	host text,
	user text,
	event_time timestamp,
	log_id UUID,
	tags set<text>,
	data text,
	PRIMARY KEY ((log_id,dc,topic_name),event_time)
);

// Create materialized view to add host as index
CREATE MATERIALIZED VIEW indexed_events AS
	SELECT log_id
	FROM event_logs
	WHERE host IS NOT NULL AND date IS NOT NULL AND dc IS NOT NULL AND event_time IS NOT NULL AND log_id IS NOT NULL AND topic_name IS NOT NULL
	PRIMARY KEY ((dc, topic_name, host),event_time,log_id)
	WITH CLUSTERING ORDER BY (event_time DESC);

// Create table to store distinct topics
CREATE TABLE IF NOT EXISTS event_topics (
	topic_name text,
	PRIMARY KEY (topic_name)
);

// Create table to store distinct dcs
CREATE TABLE IF NOT EXISTS event_dcs (
	dc text,
	PRIMARY KEY (dc)
);