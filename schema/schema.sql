DROP TABLE IF EXISTS "public"."event";

CREATE TABLE "public"."event" (
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

DROP TABLE IF EXISTS "public"."event_dc";

CREATE TABLE "public"."event_dc" (
    "dc_id" text NOT NULL,
    "dc" text,
    PRIMARY KEY ("dc_id")
);

DROP TABLE IF EXISTS "public"."event_metadata";

CREATE TABLE "public"."event_metadata" (
    "event_id" text NOT NULL,
    "data_json" json,
    PRIMARY KEY ("event_id")
);

DROP TABLE IF EXISTS "public"."event_topic";

CREATE TABLE "public"."event_topic" (
    "topic_id" uuid NOT NULL,
    "topic_name" text,
    "data_schema" json,
    PRIMARY KEY ("topic_id")
);

