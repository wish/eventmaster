package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"

	"github.com/ContextLogic/eventmaster/eventmaster"
	"github.com/gocql/gocql"
	"github.com/satori/go.uuid"
	elastic "gopkg.in/olivere/elastic.v5"
)

type Event struct {
	EventID       string                 `json:"event_id"`
	ParentEventID string                 `json:"parent_event_id"`
	EventTime     int64                  `json:"event_time"`
	Dc            string                 `json:"dc"`
	TopicName     string                 `json:"topic_name"`
	Tags          []string               `json:"tag_set"`
	Host          string                 `json:"host"`
	TargetHosts   []string               `json:"target_host"`
	User          string                 `json:"user"`
	Data          map[string]interface{} `json:"data"`
	EventType     int32                  `json:"event_type"`
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

func buildCQLInsertQuery(event *Event, data string) string {
	// TODO: change this to prevent tombstones
	return fmt.Sprintf(`
    INSERT INTO event_log (event_id, parent_event_id, dc, topic_name, host, target_host_set, user, event_time, tag_set, data_json, event_type)
    VALUES (%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]s, %[7]s, %[8]d, %[9]s, %[10]s, %[11]d);`,
		event.EventID, event.ParentEventID, stringify(event.Dc), stringify(event.TopicName),
		stringify(event.Host), stringifyArr(event.TargetHosts), stringify(event.User), event.EventTime*1000,
		stringifyArr(event.Tags), stringify(data), event.EventType)
}

type EventStore struct {
	cqlSession     *gocql.Session
	esClient       *elastic.Client
	failedEvents   []*Event
	topicNameMap   map[string]string
	topicSchemaMap map[string]string
	dcNameMap      map[string]string
}

func NewEventStore() (*EventStore, error) {
	// Establish connection to Cassandra
	dbConf := dbConfig{}
	confFile, err := ioutil.ReadFile("db_config.json")
	if err != nil {
		fmt.Println("No db_config file specified")
	} else {
		err = json.Unmarshal(confFile, &dbConf)
		if err != nil {
			fmt.Println("Error parsing db_config.json, using defaults:", err)
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
	}

	return &EventStore{
		cqlSession: session,
		esClient:   client,
	}, nil
}

func (es *EventStore) Update() {
	iter := es.cqlSession.Query("SELECT id, dc FROM event_dc;").Iter()
	var dcInfo map[string]interface{}
	for true {
		if iter.Scan(&dcInfo) {
			if dcName, ok := dcInfo["dc"].(string); ok {
				if dcId, ok := dcInfo["id"].(string); ok {
					es.dcNameMap[dcName] = dcId
				}
			}
		} else {
			break
		}
	}

	iter = es.cqlSession.Query("SELECT id, topic_name, schema FROM event_topic;").Iter()
	var topicInfo map[string]interface{}
	for true {
		if iter.Scan(&topicInfo) {
			if topicName, ok := topicInfo["topic_name"].(string); ok {
				if topicId, ok := topicInfo["id"].(string); ok {
					es.topicNameMap[topicName] = topicId
					if schema, ok := topicInfo["schema"].(string); ok {
						es.topicSchemaMap[topicId] = schema
					} else {
						es.topicSchemaMap[topicId] = ""
					}
				}
			}
		} else {
			break
		}
	}
}

func (es *EventStore) buildESQuery(q *eventmaster.Query) elastic.Query {
	var queries []elastic.Query

	if len(q.Dc) != 0 {
		dcs := make([]interface{}, 0)
		for _, dc := range q.Dc {
			dcs = append(dcs, dc)
		}
		queries = append(queries, elastic.NewTermsQuery("dc", dcs...))
	}

	if len(q.Host) != 0 {
		hosts := make([]interface{}, 0)
		for _, host := range q.Host {
			hosts = append(hosts, host)
		}
		queries = append(queries, elastic.NewTermsQuery("host", hosts...))
	}

	if len(q.TargetHost) != 0 {
		thosts := make([]interface{}, 0)
		for _, host := range q.TargetHost {
			thosts = append(thosts, host)
		}
		queries = append(queries, elastic.NewTermsQuery("target_host_set", thosts...))
	}

	if len(q.TopicName) != 0 {
		topics := make([]interface{}, 0)
		for _, topic := range q.TopicName {
			topics = append(topics, topic)
		}
		queries = append(queries, elastic.NewTermsQuery("topic_name", topics...))
	}

	if len(q.TagSet) != 0 {
		tags := make([]interface{}, 0)
		for _, tag := range q.TagSet {
			tags = append(tags, tag)
		}
		queries = append(queries, elastic.NewTermsQuery("tag_set", tags...))
	}

	// TODO: add data filters
	query := elastic.NewBoolQuery().Must(queries...)

	if q.StartTime != -1 || q.EndTime != -1 {
		rq := elastic.NewRangeQuery("event_time")
		if q.StartTime != -1 {
			rq.Gte(q.StartTime)
		}
		if q.EndTime != -1 {
			rq.Lte(q.EndTime * 1000)
		}
		query = query.Must(rq)
	}

	return query
}

func (es *EventStore) Find(q *eventmaster.Query) ([]*Event, error) {
	if q.StartTime == 0 {
		q.StartTime = -1
	}
	if q.EndTime == 0 {
		q.EndTime = -1
	}
	bq := es.buildESQuery(q)
	ctx := context.Background()

	sq := es.esClient.Search().
		Index("event_master").
		Query(bq).
		Pretty(true)

	for i, field := range q.SortField {
		sq.Sort(field, q.SortAscending[i])
	}
	sr, err := sq.Do(ctx)
	if err != nil {
		return nil, err
	}

	var evt Event
	var evts []*Event
	for _, item := range sr.Each(reflect.TypeOf(evt)) {
		if t, ok := item.(Event); ok {
			evts = append(evts, &t)
		}
	}
	return evts, nil
}

func (es *EventStore) validateEvent(event *eventmaster.Event) (bool, string) {
	if event.Dc == "" {
		return false, "Event missing dc"
	} else if event.Host == "" {
		return false, "Event missing host"
	} else if event.TopicName == "" {
		return false, "Event missing topic_name"
	} else if event.EventTime == 0 {
		return false, "Event missing event_time"
	}

	// TODO: validate event against data schema for topic
	return true, ""
}

func (es *EventStore) augmentEvent(event *eventmaster.Event) (*Event, error) {
	var d map[string]interface{}
	if event.Data != "" {
		if err := json.Unmarshal([]byte(event.Data), &d); err != nil {
			return nil, err
		}
	}
	return &Event{
		EventID:       uuid.NewV4().String(),
		ParentEventID: event.ParentEventId,
		EventTime:     event.EventTime,
		Dc:            event.Dc,
		TopicName:     event.TopicName,
		Tags:          event.TagSet,
		Host:          event.Host,
		TargetHosts:   event.TargetHostSet,
		User:          event.User,
		Data:          d,
		EventType:     event.EventType,
	}, nil
}

func (es *EventStore) AddEvent(event *eventmaster.Event) (string, error) {
	if ok, msg := es.validateEvent(event); !ok {
		return "", errors.New(msg)
	}

	evt, err := es.augmentEvent(event)
	if err != nil {
		return "", err
	}

	// write event to Cassandra
	queryStr := buildCQLInsertQuery(evt, event.Data)
	query := es.cqlSession.Query(queryStr)

	if err := query.Exec(); err != nil {
		return "", err
	}
	fmt.Println("Event added:", evt.EventID)

	// write event to Elasticsearch
	ctx := context.Background()
	_, err = es.esClient.Index().
		Index("event_master").
		Type("event").
		Id(evt.EventID).
		Timestamp(strconv.FormatInt(evt.EventTime, 10)).
		BodyJson(evt).
		Do(ctx)

	// if write to es fails, add to cache of failed event writes
	if err != nil {
		es.failedEvents = append(es.failedEvents, evt)
	}

	return evt.EventID, nil
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

func (es *EventStore) AddTopic(string name, string schema) (string, error) {
	id := uuid.NewV4().String()
	queryStr := fmt.Sprintf(`
    INSERT INTO topic (id, topic_name, schema)
    VALUES (%[1]s, %[2]s, %[3]s);`,
		id, stringify(name), stringify(schema))
	query := es.cqlSession.Query(queryStr)

	if err := query.Exec(); err != nil {
		return "", err
	}
	return id, nil
}

func (es *EventStore) AddDc(string dc) {
	id := uuid.NewV4().String()
	queryStr := fmt.Sprintf(`
    INSERT INTO dc (id, dc)
    VALUES (%[1]s, %[2]s);`,
		id, stringify(name))
	query := es.cqlSession.Query(queryStr)

	if err := query.Exec(); err != nil {
		return "", err
	}
	return id, nil
}

func (es *EventStore) CloseSession() {
	es.cqlSession.Close()
}
