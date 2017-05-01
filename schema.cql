// Create keyspace
CREATE KEYSPACE IF NOT EXISTS event_master
WITH REPLICATION = {
  'class' : 'SimpleStrategy',
	'replication_factor' : 1
};

// Use keyspace
USE event_master;

// Create event_by_topic
CREATE TABLE IF NOT EXISTS event_by_topic (
	date text,
	dc text,
	topic_name text,
	event_time timestamp,
	log_id UUID,
	PRIMARY KEY ((topic_name,date,dc),event_time)
);

// Create event_by_host
CREATE TABLE IF NOT EXISTS event_by_host (
	date text,
	dc text,
	host_name text,
	event_time timestamp,
	log_id UUID,
	PRIMARY KEY ((host_name,date,dc),event_time)
);

// Create event_by_day
CREATE TABLE IF NOT EXISTS event_by_day (
	date text,
	dc text,
	event_time timestamp,
	log_id UUID,
	PRIMARY KEY ((date,dc),event_time)
);

// Create event_logs
CREATE TABLE IF NOT EXISTS event_logs (
	date text,
	dc text,
	topic text,
	host text,
	user text,
	event_time timestamp,
	log_id UUID,
	event text,
	PRIMARY KEY (log_id,dc)
);
