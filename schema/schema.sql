DROP TABLE IF EXISTS "event";

CREATE TABLE "event" (
    "event_id" text NOT NULL,
    "parent_event_id" text,
    "dc_id" uuid,
    "topic_id" uuid,
    "host" text,
    "target_host_set" _text,
    "user" text,
    "tag_set" _text,
    "event_time" timestamptz,
    "received_time" timestamptz,
    PRIMARY KEY ("event_id")
);

-- Optional: Add BRIN index to event_time to optimize performance
CREATE INDEX event_time_idx ON event USING brin(event_time);

DROP TABLE IF EXISTS "event_dc";

CREATE TABLE "event_dc" (
    "dc_id" text NOT NULL,
    "dc" text,
    PRIMARY KEY ("dc_id")
);

DROP TABLE IF EXISTS "event_metadata";

CREATE TABLE "event_metadata" (
    "event_id" text NOT NULL,
    "data_json" json,
    PRIMARY KEY ("event_id")
);

DROP TABLE IF EXISTS "event_topic";

CREATE TABLE "event_topic" (
    "topic_id" uuid NOT NULL,
    "topic_name" text,
    "data_schema" json,
    PRIMARY KEY ("topic_id")
);

