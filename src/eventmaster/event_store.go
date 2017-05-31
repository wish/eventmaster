package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"

	"github.com/ContextLogic/eventmaster/eventmaster"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
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
	TargetHosts   []string               `json:"target_host_set"`
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

func stringifyUUID(str string) string {
	if str == "" {
		return "null"
	}
	return str
}

type EventStore struct {
	cqlSession     *gocql.Session
	esClient       *elastic.Client
	failedEvents   []*Event
	topicNameMap   map[string]string // map of name to id
	topicSchemaMap map[string]string // map of id to schema
	dcNameMap      map[string]string // map of name to id
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
		return nil, errors.Wrap(err, "Error creating cassandra session")
	}

	// Establish connection to ES and initialize index if it doesn't exist
	ctx := context.Background()
	client, err := elastic.NewClient()
	if err != nil {
		return nil, errors.Wrap(err, "Error creating elasticsearch client")
	}
	exists, err := client.IndexExists("event_master").Do(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Error finding if index exists in ES")
	}
	if !exists {
		_, err := client.CreateIndex("event_master").Do(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "Error creating event_master index in ES")
		}
	}

	return &EventStore{
		cqlSession: session,
		esClient:   client,
	}, nil
}

func (es *EventStore) getTopicId(topic string) string {
	if id, ok := es.topicNameMap[strings.ToLower(topic)]; ok {
		return id
	}
	return "null"
}

func (es *EventStore) getDcId(topic string) string {
	if id, ok := es.topicNameMap[strings.ToLower(topic)]; ok {
		return id
	}
	return "null"
}

func (es *EventStore) buildCQLInsertQuery(event *Event, data string) string {
	// TODO: change this to prevent tombstones
	return fmt.Sprintf(`
    INSERT INTO event (event_id, parent_event_id, dc, topic_name, host, target_host_set, user, event_time, tag_set, data_json, event_type)
    VALUES (%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]s, %[7]s, %[8]d, %[9]s, %[10]s, %[11]d);`,
		event.EventID, stringifyUUID(event.ParentEventID), es.getDcId(event.Dc), es.getTopicId(event.TopicName),
		stringify(event.Host), stringifyArr(event.TargetHosts), stringify(event.User), event.EventTime*1000,
		stringifyArr(event.Tags), stringify(data), event.EventType)
}

func (es *EventStore) validateSchema(schema string) bool {
	// TODO: figure out how to validate json schema
	return true
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

	id, ok := es.topicNameMap[strings.ToLower(event.TopicName)]
	if !ok {
		return false, fmt.Sprintf("Topic '%s' does not exist in topic table", strings.ToLower(event.TopicName))
	}
	schema, ok := es.topicSchemaMap[id]
	if !ok {
		return false, fmt.Sprintf("Topic %s does not have a schema defined", strings.ToLower(event.TopicName))
	}
	fmt.Println(schema)
	// TODO: validate event against data schema for topic
	return true, ""
}

func (es *EventStore) augmentEvent(event *eventmaster.Event) (*Event, error) {
	var d map[string]interface{}
	if event.Data != "" {
		if err := json.Unmarshal([]byte(event.Data), &d); err != nil {
			return nil, errors.Wrap(err, "Error unmarshalling JSON in event data")
		}
	}

	return &Event{
		EventID:       uuid.NewV4().String(),
		ParentEventID: event.ParentEventId,
		EventTime:     event.EventTime,
		Dc:            strings.ToLower(event.Dc),
		TopicName:     strings.ToLower(event.TopicName),
		Tags:          event.TagSet,
		Host:          event.Host,
		TargetHosts:   event.TargetHostSet,
		User:          event.User,
		Data:          d,
		EventType:     event.EventType,
	}, nil
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
		return nil, errors.Wrap(err, "Error executing ES search query")
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

func (es *EventStore) AddEvent(event *eventmaster.Event) (string, error) {
	if ok, msg := es.validateEvent(event); !ok {
		return "", errors.New(msg)
	}

	evt, err := es.augmentEvent(event)
	if err != nil {
		return "", errors.Wrap(err, "Error augmenting event")
	}

	// write event to Cassandra
	queryStr := es.buildCQLInsertQuery(evt, event.Data)
	query := es.cqlSession.Query(queryStr)

	if err := query.Exec(); err != nil {
		return "", errors.Wrap(err, "Error executing insert query in Cassandra")
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

func (es *EventStore) AddTopic(name string, schema string) (string, error) {
	ok := es.validateSchema(schema)
	if !ok {
		return "", errors.New("Error adding topic - schema is not in valid JSON schema format")
	}
	id := uuid.NewV4().String()
	queryStr := fmt.Sprintf(`
    INSERT INTO event_topic (topic_id, topic_name, data_schema)
    VALUES (%[1]s, %[2]s, %[3]s);`,
		id, stringify(name), stringify(schema))
	query := es.cqlSession.Query(queryStr)

	if err := query.Exec(); err != nil {
		return "", errors.Wrap(err, "Error executing insert query in Cassandra")
	}
	return id, nil
}

func (es *EventStore) UpdateTopic(oldName string, newName string, schema string) (string, error) {
	_, exists := es.topicNameMap[newName]
	if oldName != newName && exists {
		return "", errors.New(fmt.Sprintf("Error updating topic - topic with name %s already exists", newName))
	}
	id, exists := es.topicNameMap[oldName]
	if !exists {
		return "", errors.New(fmt.Sprintf("Error updating topic - topic with name %s doesn't exist", oldName))
	}
	queryStr := fmt.Sprintf(`UPDATE event_topic 
		SET topic_name = %s"`, stringify(newName))
	if schema != "" {
		ok := es.validateSchema(schema)
		if !ok {
			return "", errors.New("Error adding topic - schema is not in valid JSON schema format")
		}
		queryStr = fmt.Sprintf("%s, data_schema = %s", stringify(schema))
	}
	queryStr = fmt.Sprintf("%s WHERE topic_id = %s;", id)
	if err := es.cqlSession.Query(queryStr).Exec(); err != nil {
		return "", errors.Wrap(err, "Error executing update query in Cassandra")
	}
	es.topicNameMap[newName] = es.topicNameMap[oldName]
	if newName != oldName {
		delete(es.topicNameMap, oldName)
	}
	if schema != "" {
		es.topicSchemaMap[id] = schema
	}
	return id, nil
}

func (es *EventStore) AddDc(dc string) (string, error) {
	_, exists := es.dcNameMap[dc]
	if exists {
		return "", errors.New(fmt.Sprintf("Error adding dc - dc with name %s already exists", dc))
	}

	id := uuid.NewV4().String()
	queryStr := fmt.Sprintf(`
    INSERT INTO event_dc (dc_id, dc)
    VALUES (%[1]s, %[2]s);`,
		id, stringify(dc))
	query := es.cqlSession.Query(queryStr)

	if err := query.Exec(); err != nil {
		return "", errors.Wrap(err, "Error executing insert query in Cassandra")
	}
	return id, nil
}

func (es *EventStore) UpdateDc(oldName string, newName string) (string, error) {
	_, exists := es.dcNameMap[newName]
	if oldName != newName && exists {
		return "", errors.New(fmt.Sprintf("Error updating dc - dc with name %s already exists", newName))
	}
	id, exists := es.dcNameMap[oldName]
	if !exists {
		return "", errors.New(fmt.Sprintf("Error updating dc - dc with name %s doesn't exist", oldName))
	}
	queryStr := fmt.Sprintf(`UPDATE event_dc 
		SET dc = %s 
		WHERE dc_id = %s;`,
		stringify(newName), id)

	if err := es.cqlSession.Query(queryStr).Exec(); err != nil {
		return "", errors.Wrap(err, "Error executing update query in Cassandra")
	}
	es.dcNameMap[newName] = es.dcNameMap[oldName]
	if newName != oldName {
		delete(es.dcNameMap, oldName)
	}
	return id, nil
}

func (es *EventStore) Update() {
	var newDcNameMap map[string]string
	iter := es.cqlSession.Query("SELECT dc_id, dc FROM event_dc;").Iter()
	var dcInfo map[string]interface{}
	for true {
		if iter.Scan(&dcInfo) {
			if dcName, ok := dcInfo["dc"].(string); ok {
				if dcId, ok := dcInfo["dc_id"].(string); ok {
					newDcNameMap[dcName] = dcId
				}
			}
		} else {
			break
		}
	}
	// TODO: add mutex
	es.dcNameMap = newDcNameMap

	var newTopicNameMap map[string]string
	var newTopicSchemaMap map[string]string
	iter = es.cqlSession.Query("SELECT topic_id, topic_name, data_schema FROM event_topic;").Iter()
	var topicInfo map[string]interface{}
	for true {
		if iter.Scan(&topicInfo) {
			if topicName, ok := topicInfo["topic_name"].(string); ok {
				if topicId, ok := topicInfo["topic_id"].(string); ok {
					newTopicNameMap[topicName] = topicId
					if schema, ok := topicInfo["data_schema"].(string); ok {
						newTopicSchemaMap[topicId] = schema
					} else {
						newTopicSchemaMap[topicId] = ""
					}
				}
			}
		} else {
			break
		}
	}
	// TODO: add mutex
	es.topicNameMap = newTopicNameMap
	es.topicSchemaMap = newTopicSchemaMap
}

func (es *EventStore) CloseSession() {
	es.cqlSession.Close()
}
