package main

import (
	"context"
	"encoding/json"
	// "errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/ContextLogic/eventmaster/eventmaster"
	"github.com/gocql/gocql"
	"github.com/satori/go.uuid"
	elastic "gopkg.in/olivere/elastic.v5"
)

type Event struct {
	EventID       string   `json:"event_id"`
	ParentEventID string   `json:"parent_event_id"`
	EventTime     int64    `json:"event_time"`
	Dc            string   `json:"dc"`
	TopicName     string   `json:"topic_name`
	Tags          []string `json:"tag_set"`
	Host          string   `json:"host"`
	TargetHosts   []string `json:"target_host"`
	User          string   `json:"user"`
	DataJSON      string   `json:"data_json"`
	EventType     int32    `json:"event_type"`
}

func stringify(str string) string {
	if str == "" {
		return "null"
	}
	return fmt.Sprintf("'%s'", str)
}

func stringifyArr(arr []string) string {
	if len(arr) == 0 {
		return "null"
	}
	for i, str := range arr {
		arr[i] = stringify(str)
	}
	return fmt.Sprintf("{%s}", strings.Join(arr, ","))
}

func buildCQLInsertQuery(event *Event) string {
	// TODO: change this to prevent tombstones
	return fmt.Sprintf(`
	BEGIN BATCH
    INSERT INTO event_logs (event_id, parent_event_id, dc, topic_name, host, target_host_set, user, event_time, tag_set, data_json, event_type)
    VALUES (%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]s, %[7]s, %[8]d, %[9]s, %[10]s, %[11]d);
    INSERT INTO event_topics (topic_name)
    VALUES (%[4]s);
    INSERT INTO event_dcs (dc)
    VALUES (%[3]s)
    APPLY BATCH`,
		event.EventID, event.ParentEventID, stringify(event.Dc), stringify(event.TopicName),
		stringify(event.Host), stringifyArr(event.TargetHosts), stringify(event.User), event.EventTime*1000,
		stringifyArr(event.Tags), stringify(event.DataJSON), event.EventType)
}

type EventStore struct {
	cqlSession *gocql.Session
	esClient   *elastic.Client
}

func NewEventStore() (*EventStore, error) {
	// Establish connection to Cassandra
	dbConf := dbConfig{}
	confFile, err := ioutil.ReadFile("db_config.json")
	if err != nil {
		fmt.Println("no db_config file specified")
	} else {
		err = json.Unmarshal(confFile, &dbConf)
		if err != nil {
			fmt.Println("error parsing db_config.json:", err)
		}
	}
	if dbConf.Host == "" {
		dbConf.Host = "127.0.0.1"
	}
	if dbConf.Port == "" {
		dbConf.Port = "9042"
	}
	if dbConf.Keyspace == "" {
		dbConf.Keyspace = "event_master"
	}
	if dbConf.Consistency == "" {
		dbConf.Consistency = "quorum"
	}
	cluster := gocql.NewCluster(fmt.Sprintf("%s:%s", dbConf.Host, dbConf.Port))
	cluster.Keyspace = dbConf.Keyspace

	if dbConf.Consistency == "one" {
		cluster.Consistency = gocql.One
	} else if dbConf.Consistency == "two" {
		cluster.Consistency = gocql.Two
	} else {
		cluster.Consistency = gocql.Quorum
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	// Establish connection to ES and initialize index if it doesn't exist
	ctx := context.Background()
	client, err := elastic.NewClient()
	if err != nil {
		return nil, err
	}
	exists, err := client.IndexExists("event_master").Do(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		_, err := client.CreateIndex("event_master").Do(ctx)
		if err != nil {
			return nil, err
		}
		mappingFile, err := ioutil.ReadFile("event_type_mapping.json")
		if err != nil {
			return nil, err
		}
		res, err := elastic.NewIndicesPutMappingService(client).
			Index("event_master").
			Type("event").
			BodyString(string(mappingFile)).
			Do(ctx)
		if err != nil {
			return nil, err
		} else if !res.Acknowledged {
			fmt.Println("did not acknowledge put mapping")
		}
	}

	return &EventStore{
		cqlSession: session,
		esClient:   client,
	}, nil
}

func (es *EventStore) buildESQuery(q *eventmaster.Query) elastic.Query {
	var queries []elastic.Query

	if len(q.Dc) != 0 {
		queries = append(queries, elastic.NewMatchQuery("dc", q.Dc))
	}

	if len(q.Host) != 0 {
		queries = append(queries, elastic.NewMatchQuery("host", q.Host))
	}

	if len(q.TargetHost) != 0 {
		queries = append(queries, elastic.NewMatchQuery("target_host_set", q.TargetHost))
	}

	if len(q.TopicName) != 0 {
		queries = append(queries, elastic.NewMatchQuery("topic_name", q.TopicName))
	}

	if len(q.TagSet) != 0 {
		queries = append(queries, elastic.NewMatchQuery("tag_set", q.TagSet))
	}

	startTime, endTime := int64(0), int64(time.Now().Unix()*1000)
	if q.StartTime != -1 {
		startTime = q.StartTime
	}
	if q.EndTime != -1 {
		endTime = q.EndTime * 1000
	}
	rq := elastic.NewRangeQuery("event_time").Gte(startTime).Lte(endTime)
	return elastic.NewBoolQuery().
		Must(queries...).
		Must(rq)
}

func (es *EventStore) Find(q *eventmaster.Query) ([]*Event, error) {
	bq := es.buildESQuery(q)
	ctx := context.Background()
	sr, err := es.esClient.Search().
		Index("event_master").
		Query(bq).
		Pretty(true).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Println(sr)
	var evts []*Event
	return evts, nil
}

func (es *EventStore) AddEvent(event *Event) error {
	event.EventID = uuid.NewV4().String()

	// write event to Cassandra
	queryStr := buildCQLInsertQuery(event)
	query := es.cqlSession.Query(queryStr)

	if err := query.Exec(); err != nil {
		return err
	}
	fmt.Println("Event added:", event.EventID)

	// write event to Elasticsearch
	ctx := context.Background()
	_, err := es.esClient.Index().
		Index("event_master").
		Type("event").
		Id(event.EventID).
		BodyJson(event).
		TTL("1000").
		Do(ctx)

	return err
}

func (es *EventStore) GetTopics() []string {
	iter := es.cqlSession.Query("SELECT topic_name FROM event_topics;").Iter()
	var topic string
	var topics []string
	for true {
		if iter.Scan(&topic) {
			topics = append(topics, topic)
		} else {
			break
		}
	}
	return topics
}

func (es *EventStore) GetDcs() []string {
	iter := es.cqlSession.Query("SELECT dc FROM event_dcs;").Iter()
	var dc string
	var dcs []string
	for true {
		if iter.Scan(&dc) {
			dcs = append(dcs, dc)
		} else {
			break
		}
	}
	return dcs
}

func (es *EventStore) CloseSession() {
	es.cqlSession.Close()
}
