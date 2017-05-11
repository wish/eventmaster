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
	tags list<text>,
	data text,
	PRIMARY KEY ((log_id,dc), event_time)
);

CREATE MATERIALIZED VIEW event_by_topic AS
	SELECT topic_name, tags, date, dc, event_time, log_id
	FROM event_logs
	WHERE topic_name IS NOT NULL AND date IS NOT NULL AND dc IS NOT NULL AND event_time IS NOT NULL AND log_id IS NOT NULL
	PRIMARY KEY ((topic_name,dc),event_time,log_id);

CREATE MATERIALIZED VIEW event_by_host AS
	SELECT date, dc, host, user, event_time, log_id
	FROM event_logs
	WHERE host IS NOT NULL AND date IS NOT NULL AND dc IS NOT NULL AND event_time IS NOT NULL AND log_id IS NOT NULL
	PRIMARY KEY ((host,dc),event_time,log_id);

CREATE MATERIALIZED VIEW event_by_day AS
	SELECT date, dc, event_time, log_id
	FROM event_logs
	WHERE date IS NOT NULL AND dc IS NOT NULL AND event_time IS NOT NULL AND log_id IS NOT NULL
	PRIMARY KEY ((date,dc),event_time,log_id);
