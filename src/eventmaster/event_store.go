package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ContextLogic/eventmaster/eventmaster"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/satori/go.uuid"
	"github.com/xeipuuv/gojsonschema"
	elastic "gopkg.in/olivere/elastic.v5"
)

type Pair struct {
	a string
	b interface{}
}

// stringify* used for mapping to cassandra inputs
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

func getIndexFromTime(evtTime int64) string {
	eventTime := time.Unix(evtTime, 0)
	return fmt.Sprintf("eventmaster_%04d_%02d_%02d", eventTime.Year(), eventTime.Month(), eventTime.Day())
}

func getDataQueries(data map[string]interface{}) []Pair {
	var pairs []Pair
	for k, v := range data {
		m, ok := v.(map[string]interface{})
		if ok {
			nextPairs := getDataQueries(m)
			for _, pair := range nextPairs {
				pairs = append(pairs, Pair{fmt.Sprintf("%s.%s", k, pair.a), pair.b})
			}
		} else {
			pairs = append(pairs, Pair{k, v})
		}
	}
	return pairs
}

func insertDefaults(schema map[string]interface{}, m map[string]interface{}) {
	for k, v := range schema {
		s, ok := v.(map[string]interface{})
		if !ok {
			continue
		}

		if d, ok := s["default"]; ok {
			m[k] = d
		} else if property, ok := m[k]; ok {
			// property exists, check inner objects
			if innerM, ok := property.(map[string]interface{}); ok {
				innerProperties := s["properties"]
				if innerSchema, ok := innerProperties.(map[string]interface{}); ok {
					insertDefaults(innerSchema, innerM)
				}
			}
		} else if properties, ok := schema["properties"]; ok {
			// property doesn't exist, create it and check inner levels
			innerProperties := properties.(map[string]interface{})
			innerMap := make(map[string]interface{})
			insertDefaults(innerProperties, innerMap)
			if len(innerMap) != 0 {
				m[k] = innerMap
			}
		}
	}
}

func checkBackwardsCompatible(oldSchema map[string]interface{}, newSchema map[string]interface{}) bool {
	oldProperties := oldSchema["properties"]
	oldP, _ := oldProperties.(map[string]interface{})
	oldRequired := oldSchema["required"]
	oldR, _ := oldRequired.([]interface{})

	newProperties := newSchema["properties"]
	newP, _ := newProperties.(map[string]interface{})
	newRequired := newSchema["required"]
	newR, _ := newRequired.([]interface{})

	// get diff of new required properties, check if those have defaults
	for _, p := range newR {
		exists := false
		for _, oldP := range oldR {
			if oldP == p {
				exists = true
			}
		}

		if !exists {
			prop := p.(string)
			property := newP[prop]
			if typedProperty, ok := property.(map[string]interface{}); ok {
				if _, ok := typedProperty["default"]; !ok {
					return false
				}
			} else {
				return false
			}
		}
	}

	// check backwards compatibility for all nested objects
	for k, v := range newP {
		if innerP, ok := v.(map[string]interface{}); ok {
			oldInnerP := oldP[k]
			typedOldInnerP := oldInnerP.(map[string]interface{})
			if !checkBackwardsCompatible(typedOldInnerP, innerP) {
				return false
			}
		}
	}
	return true
}

type Event struct {
	EventID       string                 `json:"event_id"`
	ParentEventID string                 `json:"parent_event_id"`
	EventTime     int64                  `json:"event_time"`
	DcID          string                 `json:"dc_id"`
	TopicID       string                 `json:"topic_id"`
	Tags          []string               `json:"tag_set"`
	Host          string                 `json:"host"`
	TargetHosts   []string               `json:"target_host_set"`
	User          string                 `json:"user"`
	Data          map[string]interface{} `json:"data"`
	ReceivedTime  int64                  `json:"received_time"`
}

func (event *Event) toCassandra(data string) string {
	// TODO: change this to prevent tombstones
	return fmt.Sprintf(`
	BEGIN BATCH
    INSERT INTO event (event_id, parent_event_id, dc_id, topic_id, host, target_host_set, user, event_time, tag_set, data_json, received_time)
    VALUES (%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]s, %[7]s, %[8]d, %[9]s, %[10]s, %[11]d);
    INSERT INTO temp_event (event_id, parent_event_id, dc_id, topic_id, host, target_host_set, user, event_time, tag_set, data_json, received_time)
    VALUES (%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]s, %[7]s, %[8]d, %[9]s, %[10]s, %[11]d);
    APPLY BATCH;`,
		event.EventID, stringifyUUID(event.ParentEventID), stringifyUUID(event.DcID), stringifyUUID(event.TopicID),
		stringify(event.Host), stringifyArr(event.TargetHosts), stringify(event.User), event.EventTime*1000,
		stringifyArr(event.Tags), stringify(data), event.ReceivedTime)
}

type TopicData struct {
	Name   string `json:"topic_name"`
	Schema string `json:"data_schema"`
}

type DcData struct {
	Name string `json:"dc"`
}

type EventStore struct {
	cqlSession               *gocql.Session
	esClient                 *elastic.Client
	topicNameToId            map[string]string                   // map of name to id
	topicIdToName            map[string]string                   // map of id to name
	topicSchemaMap           map[string]*gojsonschema.Schema     // map of topic id to json loader for schema validation
	topicSchemaPropertiesMap map[string](map[string]interface{}) // map of topic id to properties of topic data
	dcNameToId               map[string]string                   // map of name to id
	dcIdToName               map[string]string                   // map of id to name
	indexNames               []string                            // list of name of all indices in es cluster
	topicMutex               *sync.RWMutex
	dcMutex                  *sync.RWMutex
	indexMutex               *sync.RWMutex
	registry                 metrics.Registry
}

func NewEventStore(dbConf dbConfig, registry metrics.Registry) (*EventStore, error) {
	// Establish connection to Cassandra
	cluster := gocql.NewCluster(dbConf.CassandraAddr)
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
	client, err := elastic.NewClient(elastic.SetURL(dbConf.ESAddr))
	if err != nil {
		return nil, errors.Wrap(err, "Error creating elasticsearch client")
	}

	return &EventStore{
		cqlSession: session,
		esClient:   client,
		topicMutex: &sync.RWMutex{},
		dcMutex:    &sync.RWMutex{},
		indexMutex: &sync.RWMutex{},
		registry:   registry,
	}, nil
}

func (es *EventStore) getTopicId(topic string) string {
	es.topicMutex.RLock()
	id := es.topicNameToId[strings.ToLower(topic)]
	es.topicMutex.RUnlock()
	return id
}

func (es *EventStore) getTopicName(id string) string {
	es.topicMutex.RLock()
	name := es.topicIdToName[id]
	es.topicMutex.RUnlock()
	return name
}

func (es *EventStore) getTopicSchema(id string) *gojsonschema.Schema {
	es.topicMutex.RLock()
	schema := es.topicSchemaMap[id]
	es.topicMutex.RUnlock()
	return schema
}

func (es *EventStore) getTopicSchemaProperties(id string) map[string]interface{} {
	es.topicMutex.RLock()
	schema := es.topicSchemaPropertiesMap[id]
	es.topicMutex.RUnlock()
	return schema
}

func (es *EventStore) getDcId(dc string) string {
	es.dcMutex.RLock()
	id := es.dcNameToId[strings.ToLower(dc)]
	es.dcMutex.RUnlock()
	return id
}

func (es *EventStore) getDcName(id string) string {
	es.dcMutex.RLock()
	name := es.dcIdToName[id]
	es.dcMutex.RUnlock()
	return name
}

func (es *EventStore) getESIndices() []string {
	es.indexMutex.RLock()
	names := es.indexNames
	es.indexMutex.RUnlock()
	return names
}

func (es *EventStore) validateSchema(schema string) (*gojsonschema.Schema, bool) {
	loader := gojsonschema.NewStringLoader(schema)
	jsonSchema, err := gojsonschema.NewSchema(loader)
	if err != nil {
		return nil, false
	}
	return jsonSchema, true
}

func (es *EventStore) buildESQuery(q *eventmaster.Query) elastic.Query {
	var queries []elastic.Query

	if len(q.Dc) != 0 {
		dcs := make([]interface{}, 0)
		for _, dc := range q.Dc {
			if id := es.getDcId(strings.ToLower(dc)); id != "" {
				dcs = append(dcs, id)
			}
		}
		queries = append(queries, elastic.NewTermsQuery("dc_id", dcs...))
	}
	if len(q.Host) != 0 {
		hosts := make([]interface{}, 0)
		for _, host := range q.Host {
			hosts = append(hosts, host)
		}
		queries = append(queries, elastic.NewTermsQuery("host", hosts...))
	}
	if len(q.TargetHostSet) != 0 {
		thosts := make([]interface{}, 0)
		for _, host := range q.TargetHostSet {
			thosts = append(thosts, host)
		}
		queries = append(queries, elastic.NewTermsQuery("target_host_set", thosts...))
	}
	if len(q.TopicName) != 0 {
		topics := make([]interface{}, 0)
		for _, topic := range q.TopicName {
			if id := es.getTopicId(strings.ToLower(topic)); id != "" {
				topics = append(topics, id)
			}
		}
		queries = append(queries, elastic.NewTermsQuery("topic_id", topics...))
	}
	if len(q.TagSet) != 0 {
		tags := make([]interface{}, 0)
		for _, tag := range q.TagSet {
			tags = append(tags, tag)
		}
		queries = append(queries, elastic.NewTermsQuery("tag_set", tags...))
	}
	if q.Data != "" {
		var d map[string]interface{}
		if err := json.Unmarshal([]byte(q.Data), &d); err != nil {
			fmt.Println("Ignoring data filters - not in valid JSON format")
		} else {
			dataQueries := getDataQueries(d)
			for _, pair := range dataQueries {
				queries = append(queries, elastic.NewTermQuery(fmt.Sprintf("%s.%s", "data", pair.a), pair.b))
			}
		}
	}

	query := elastic.NewBoolQuery().Must(queries...)

	if q.StartEventTime != 0 || q.EndEventTime != 0 {
		rq := elastic.NewRangeQuery("event_time")
		if q.StartEventTime != -1 {
			rq.Gte(q.StartEventTime)
		}
		if q.EndEventTime != -1 {
			rq.Lte(q.EndEventTime * 1000)
		}
		query = query.Must(rq)
	}

	if q.StartReceivedTime != 0 || q.EndReceivedTime != 0 {
		rq := elastic.NewRangeQuery("received_time")
		if q.StartReceivedTime != -1 {
			rq.Gte(q.StartReceivedTime)
		}
		if q.EndReceivedTime != -1 {
			rq.Lte(q.EndReceivedTime * 1000)
		}
		query = query.Must(rq)
	}

	return query
}

func (es *EventStore) insertDefaults(s map[string]interface{}, m map[string]interface{}) {
	properties := s["properties"]
	p, _ := properties.(map[string]interface{})
	insertDefaults(p, m)
}

func (es *EventStore) augmentEvent(event *eventmaster.Event) (*Event, error) {
	// validate Event
	if event.Dc == "" {
		return nil, errors.New("Event missing dc")
	} else if event.Host == "" {
		return nil, errors.New("Event missing host")
	} else if event.TopicName == "" {
		return nil, errors.New("Event missing topic_name")
	}

	if event.EventTime == 0 {
		event.EventTime = time.Now().Unix()
	}
	if event.Data == "" {
		event.Data = "{}"
	}

	dcID := es.getDcId(strings.ToLower(event.Dc))
	if dcID == "" {
		return nil, errors.New(fmt.Sprintf("Dc '%s' does not exist in dc table", strings.ToLower(event.Dc)))
	}
	topicID := es.getTopicId(strings.ToLower(event.TopicName))
	if topicID == "" {
		return nil, errors.New(fmt.Sprintf("Topic '%s' does not exist in topic table", strings.ToLower(event.TopicName)))
	}

	topicSchema := es.getTopicSchema(topicID)
	var d map[string]interface{}
	if topicSchema != nil {
		propertiesSchema := es.getTopicSchemaProperties(topicID)
		if err := json.Unmarshal([]byte(event.Data), &d); err != nil {
			return nil, errors.Wrap(err, "Error unmarshalling JSON in event data")
		}
		es.insertDefaults(propertiesSchema, d)
		dataJson, err := json.Marshal(d)
		if err != nil {
			return nil, errors.Wrap(err, "Error marshalling data with defaults into json")
		}
		dataLoader := gojsonschema.NewBytesLoader(dataJson)
		result, err := topicSchema.Validate(dataLoader)
		if err != nil {
			return nil, errors.Wrap(err, "Error validating event data against schema")
		}
		if !result.Valid() {
			errMsg := ""
			for _, err := range result.Errors() {
				errMsg = fmt.Sprintf("%s, %s", errMsg, err)
			}
			return nil, errors.New(errMsg)
		}
	}

	return &Event{
		EventID:       uuid.NewV4().String(),
		ParentEventID: event.ParentEventId,
		EventTime:     event.EventTime,
		DcID:          dcID,
		TopicID:       topicID,
		Tags:          event.TagSet,
		Host:          event.Host,
		TargetHosts:   event.TargetHostSet,
		User:          event.User,
		Data:          d,
		ReceivedTime:  time.Now().Unix(),
	}, nil
}

func (es *EventStore) checkIndex(index string) error {
	ctx := context.Background()
	exists, err := es.esClient.IndexExists(index).Do(ctx)
	if err != nil {
		return errors.Wrap(err, "Error finding if index exists in ES")
	}
	if !exists {
		_, err := es.esClient.CreateIndex(index).Do(ctx)
		if err != nil {
			return errors.Wrap(err, "Error creating event_master index in ES")
		}
	}
	return nil
}

func (es *EventStore) Find(q *eventmaster.Query) ([]*Event, error) {
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("Find", es.registry)
	defer timer.UpdateSince(start)

	bq := es.buildESQuery(q)
	ctx := context.Background()

	indexNames := es.getESIndices()

	if q.StartEventTime != 0 {
		startIndex := getIndexFromTime(q.StartEventTime)
		for i := len(indexNames) - 1; i >= 0; i-- {
			if strings.Compare(indexNames[i], startIndex) == -1 {
				indexNames[i] = indexNames[len(indexNames)-1]
				indexNames = indexNames[:len(indexNames)-1]
			}
		}
	}
	if q.EndEventTime != 0 {
		endIndex := getIndexFromTime(q.EndEventTime)
		for i := len(indexNames) - 1; i >= 0; i-- {
			if strings.Compare(indexNames[i], endIndex) == 1 {
				indexNames[i] = indexNames[len(indexNames)-1]
				indexNames = indexNames[:len(indexNames)-1]
			}
		}
	}

	var evts []*Event
	if len(indexNames) == 0 {
		return evts, nil
	}

	limit := q.Limit
	if limit == 0 {
		limit = 50
	}

	sq := es.esClient.Search().
		Index(indexNames...).
		Query(bq).
		From(int(q.Start)).Size(int(limit)).
		Pretty(true)

	for i, field := range q.SortField {
		sq.Sort(field, q.SortAscending[i])
	}
	sr, err := sq.Do(ctx)
	if err != nil {
		esMeter := metrics.GetOrRegisterMeter("esSearchError", es.registry)
		esMeter.Mark(1)
		return nil, errors.Wrap(err, "Error executing ES search query")

	}
	eventsByTopic := make(map[string][]*Event)

	var evt Event
	for _, item := range sr.Each(reflect.TypeOf(evt)) {
		if e, ok := item.(Event); ok {
			topicID := e.TopicID
			eventsByTopic[topicID] = append(eventsByTopic[topicID], &e)
		}
	}

	for topicID, events := range eventsByTopic {
		propertiesSchema := es.getTopicSchemaProperties(topicID)
		if propertiesSchema != nil || len(propertiesSchema) != 0 {
			for i, _ := range events {
				es.insertDefaults(propertiesSchema, events[i].Data)
			}
		}
		evts = append(evts, events...)
	}
	return evts, nil
}

func (es *EventStore) AddEvent(event *eventmaster.Event) (string, error) {
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("AddEvent", es.registry)
	defer timer.UpdateSince(start)

	evt, err := es.augmentEvent(event)
	if err != nil {
		return "", errors.Wrap(err, "Error augmenting event")
	}
	// write event to Cassandra
	queryStr := evt.toCassandra(event.Data)
	query := es.cqlSession.Query(queryStr)
	if err := query.Exec(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraWriteError", es.registry)
		cassMeter.Mark(1)
		return "", errors.Wrap(err, "Error executing insert query in Cassandra")
	}
	fmt.Println("Event added:", evt.EventID)
	return evt.EventID, nil
}

func (es *EventStore) GetTopics() ([]TopicData, error) {
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("GetTopics", es.registry)
	defer timer.UpdateSince(start)

	iter := es.cqlSession.Query("SELECT topic_name, data_schema FROM event_topic;").Iter()
	var name, schema string
	var topics []TopicData
	for {
		if iter.Scan(&name, &schema) {
			topics = append(topics, TopicData{
				Name:   name,
				Schema: schema,
			})
		} else {
			break
		}
	}
	if err := iter.Close(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraReadError", es.registry)
		cassMeter.Mark(1)
		return nil, errors.Wrap(err, "Error closing iter")
	}
	return topics, nil
}

func (es *EventStore) GetDcs() ([]string, error) {
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("GetDcs", es.registry)
	defer timer.UpdateSince(start)

	iter := es.cqlSession.Query("SELECT dc FROM event_dc;").Iter()
	var dc string
	var dcs []string
	for true {
		if iter.Scan(&dc) {
			dcs = append(dcs, dc)
		} else {
			break
		}
	}
	if err := iter.Close(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraReadError", es.registry)
		cassMeter.Mark(1)
		return nil, errors.Wrap(err, "Error closing iter")
	}
	return dcs, nil
}

func (es *EventStore) AddTopic(name string, schema string) (string, error) {
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("AddTopic", es.registry)
	defer timer.UpdateSince(start)

	var s map[string]interface{}
	var jsonSchema *gojsonschema.Schema
	if schema != "" {
		var ok bool
		jsonSchema, ok = es.validateSchema(schema)
		if !ok {
			return "", errors.New("Error adding topic - schema is not in valid JSON format")
		}

		if err := json.Unmarshal([]byte(schema), &s); err != nil {
			return "", errors.Wrap(err, "Error unmarshalling json schema")
		}
	}

	id := uuid.NewV4().String()
	queryStr := fmt.Sprintf(`
    INSERT INTO event_topic (topic_id, topic_name, data_schema)
    VALUES (%[1]s, %[2]s, %[3]s);`,
		id, stringify(name), stringify(schema))
	query := es.cqlSession.Query(queryStr)

	if err := query.Exec(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraWriteError", es.registry)
		cassMeter.Mark(1)
		return "", errors.Wrap(err, "Error executing insert query in Cassandra")
	}
	fmt.Println("Topic Added:", name, id)

	es.topicMutex.Lock()
	es.topicNameToId[name] = id
	es.topicIdToName[id] = name
	es.topicSchemaPropertiesMap[id] = s
	es.topicSchemaMap[id] = jsonSchema
	es.topicMutex.Unlock()
	return id, nil
}

func (es *EventStore) UpdateTopic(oldName string, newName string, schema string) (string, error) {
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("UpdateTopic", es.registry)
	defer timer.UpdateSince(start)

	if newName == "" {
		newName = oldName
	}

	id := es.getTopicId(newName)
	if oldName != newName && id != "" {
		return "", errors.New(fmt.Sprintf("Error updating topic - topic with name %s already exists", newName))
	}
	id = es.getTopicId(oldName)
	if id == "" {
		return "", errors.New(fmt.Sprintf("Error updating topic - topic with name %s doesn't exist", oldName))
	}
	queryStr := fmt.Sprintf(`UPDATE event_topic 
		SET topic_name = %s"`, stringify(newName))

	var jsonSchema *gojsonschema.Schema
	var ok bool
	var s map[string]interface{}
	if schema != "" {
		jsonSchema, ok = es.validateSchema(schema)
		if !ok {
			return "", errors.New("Error adding topic - schema is not in valid JSON schema format")
		}
		if err := json.Unmarshal([]byte(schema), &s); err != nil {
			return "", errors.Wrap(err, "Error unmarshalling json schema")
		}

		old := es.getTopicSchemaProperties(id)
		ok = checkBackwardsCompatible(old, s)
		if !ok {
			return "", errors.New("Error adding topic - new schema is not backwards compatible")
		}
	}

	queryStr = fmt.Sprintf("%s WHERE topic_id = %s;", queryStr, id)
	if err := es.cqlSession.Query(queryStr).Exec(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraWriteError", es.registry)
		cassMeter.Mark(1)
		return "", errors.Wrap(err, "Error executing update query in Cassandra")
	}

	es.topicMutex.Lock()
	es.topicNameToId[newName] = es.topicNameToId[oldName]
	es.topicIdToName[id] = newName
	if newName != oldName {
		delete(es.topicNameToId, oldName)
	}
	es.topicSchemaMap[id] = jsonSchema
	if schema != "" {
		es.topicSchemaPropertiesMap[id] = s
	}
	es.topicMutex.Unlock()
	return id, nil
}

func (es *EventStore) DeleteTopic(topicName string) error {
	id := es.getTopicId(topicName)
	if id == "" {
		return errors.New("Couldn't find topic id for topic:" + topicName)
	}
	if _, err := es.esClient.DeleteByQuery(es.getESIndices()...).
		Query(elastic.NewTermQuery("topic_id", topicName)).
		Do(context.Background()); err != nil {
		return errors.Wrap(err, "Error deleting events under topic from ES")
	}

	if err := es.cqlSession.Query(fmt.Sprintf(`BEGIN BATCH
		DELETE FROM event WHERE topic_id=%[1]s;
		DELETE FROM event_topic WHERE topic_id=%[1]s;
		APPLY BATCH;`, id)).Exec(); err != nil {
		return errors.Wrap(err, "Error deleting topic and its events from cassandra")
	}
	es.topicMutex.Lock()
	delete(es.topicNameToId, topicName)
	delete(es.topicIdToName, id)
	delete(es.topicSchemaMap, id)
	delete(es.topicSchemaPropertiesMap, id)
	es.topicMutex.Unlock()
	return nil
}

func (es *EventStore) AddDc(dc string) (string, error) {
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("AddDc", es.registry)
	defer timer.UpdateSince(start)

	if dc == "" {
		return "", errors.New("Error adding dc - dc name is empty")
	}
	id := es.getDcId(dc)
	if id != "" {
		return "", errors.New(fmt.Sprintf("Error adding dc - dc with name %s already exists", dc))
	}

	id = uuid.NewV4().String()
	queryStr := fmt.Sprintf(`
    INSERT INTO event_dc (dc_id, dc)
    VALUES (%[1]s, %[2]s);`,
		id, stringify(dc))
	query := es.cqlSession.Query(queryStr)

	if err := query.Exec(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraWriteError", es.registry)
		cassMeter.Mark(1)
		return "", errors.Wrap(err, "Error executing insert query in Cassandra")
	}
	fmt.Println("Dc Added:", dc, id)

	es.dcMutex.Lock()
	es.dcIdToName[id] = dc
	es.dcNameToId[dc] = id
	es.dcMutex.Unlock()
	return id, nil
}

func (es *EventStore) UpdateDc(oldName string, newName string) (string, error) {
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("UpdateDc", es.registry)
	defer timer.UpdateSince(start)

	if newName == "" {
		newName = oldName
	}

	id := es.getDcId(newName)
	if oldName != newName && id != "" {
		return "", errors.New(fmt.Sprintf("Error updating dc - dc with name %s already exists", newName))
	}
	id = es.getDcId(oldName)
	if id == "" {
		return "", errors.New(fmt.Sprintf("Error updating dc - dc with name %s doesn't exist", oldName))
	}
	queryStr := fmt.Sprintf(`UPDATE event_dc 
		SET dc = %s 
		WHERE dc_id = %s;`,
		stringify(newName), id)

	if err := es.cqlSession.Query(queryStr).Exec(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraWriteError", es.registry)
		cassMeter.Mark(1)
		return "", errors.Wrap(err, "Error executing update query in Cassandra")
	}
	es.dcMutex.Lock()
	es.dcNameToId[newName] = es.dcNameToId[oldName]
	es.dcIdToName[id] = newName
	if newName != oldName {
		delete(es.dcNameToId, oldName)
	}
	es.dcMutex.Unlock()
	return id, nil
}

func (es *EventStore) Update() error {
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("Update", es.registry)
	defer timer.UpdateSince(start)

	newDcNameToId := make(map[string]string)
	newDcIdToName := make(map[string]string)
	iter := es.cqlSession.Query("SELECT dc_id, dc FROM event_dc;").Iter()
	var dcId gocql.UUID
	var dcName string
	for true {
		if iter.Scan(&dcId, &dcName) {
			newDcNameToId[dcName] = dcId.String()
			newDcNameToId[dcId.String()] = dcName
		} else {
			break
		}
	}
	if err := iter.Close(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraReadError", es.registry)
		cassMeter.Mark(1)
		return errors.Wrap(err, "Error closing dc iter")
	}
	if newDcNameToId != nil {
		es.dcMutex.Lock()
		es.dcNameToId = newDcNameToId
		es.dcIdToName = newDcIdToName
		es.dcMutex.Unlock()
	}

	newTopicNameToId := make(map[string]string)
	newTopicIdToName := make(map[string]string)
	schemaMap := make(map[string]string)
	newTopicSchemaMap := make(map[string]*gojsonschema.Schema)
	newTopicSchemaPropertiesMap := make(map[string](map[string]interface{}))
	iter = es.cqlSession.Query("SELECT topic_id, topic_name, data_schema FROM event_topic;").Iter()
	var topicId gocql.UUID
	var topicName, dataSchema string
	for true {
		if iter.Scan(&topicId, &topicName, &dataSchema) {
			id := topicId.String()
			newTopicNameToId[topicName] = id
			newTopicIdToName[id] = topicName
			schemaMap[id] = dataSchema
		} else {
			break
		}
	}
	if err := iter.Close(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraReadError", es.registry)
		cassMeter.Mark(1)
		return errors.Wrap(err, "Error closing topic iter")
	}
	for id, schema := range schemaMap {
		if schema != "" {
			var s map[string]interface{}
			if err := json.Unmarshal([]byte(schema), &s); err != nil {
				return errors.Wrap(err, "Error unmarshalling json schema")
			}

			schemaLoader := gojsonschema.NewStringLoader(schema)
			jsonSchema, err := gojsonschema.NewSchema(schemaLoader)
			if err != nil {
				return errors.Wrap(err, "Error validating schema for topic "+id)
			}
			newTopicSchemaMap[id] = jsonSchema
			newTopicSchemaPropertiesMap[id] = s
		}
	}
	es.topicMutex.Lock()
	es.topicNameToId = newTopicNameToId
	es.topicIdToName = newTopicIdToName
	es.topicSchemaMap = newTopicSchemaMap
	es.topicSchemaPropertiesMap = newTopicSchemaPropertiesMap
	es.topicMutex.Unlock()

	indices, err := es.esClient.IndexNames()
	if err != nil {
		esMeter := metrics.GetOrRegisterMeter("esReadError", es.registry)
		esMeter.Mark(1)
		return errors.Wrap(err, "Error getting index names in ES")
	}
	if indices != nil {
		es.indexMutex.Lock()
		es.indexNames = indices
		es.indexMutex.Unlock()
	}
	return nil
}

func (es *EventStore) FlushToES() error {
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("FlushToES", es.registry)
	defer timer.UpdateSince(start)

	iter := es.cqlSession.Query(`SELECT event_id, parent_event_id, dc_id, topic_id,
		host, target_host_set, user, event_time, tag_set, data_json, received_time
		FROM temp_event LIMIT 1000;`).Iter()
	var eventID, parentEventID, topicID, dcID gocql.UUID
	var eventTime, receivedTime int64
	var host, user, data string
	var targetHostSet, tagSet []string

	// keep track of event and topic IDs to generate delete query from cassandra
	var eventIDs []string
	topicIDs := make(map[string]struct{})
	esIndices := make(map[string]([]*Event))
	var v struct{}
	for true {
		if iter.Scan(&eventID, &parentEventID, &dcID, &topicID, &host, &targetHostSet, &user, &eventTime, &tagSet, &data, &receivedTime) {
			var d map[string]interface{}
			if data != "" {
				if err := json.Unmarshal([]byte(data), &d); err != nil {
					return errors.Wrap(err, "Error unmarshalling JSON in event data")
				}
			}
			eventIDs = append(eventIDs, eventID.String())
			topicIDs[topicID.String()] = v
			parentEventIDStr := parentEventID.String()
			if parentEventIDStr == "00000000-0000-0000-0000-000000000000" {
				parentEventIDStr = ""
			}
			index := getIndexFromTime(eventTime / 1000)
			esIndices[index] = append(esIndices[index], &Event{
				EventID:       eventID.String(),
				ParentEventID: parentEventIDStr,
				EventTime:     eventTime,
				DcID:          dcID.String(),
				TopicID:       topicID.String(),
				Tags:          tagSet,
				Host:          host,
				TargetHosts:   targetHostSet,
				User:          user,
				Data:          d,
				ReceivedTime:  receivedTime,
			})
		} else {
			break
		}
	}
	if err := iter.Close(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraReadError", es.registry)
		cassMeter.Mark(1)
		return errors.Wrap(err, "Error closing iter")
	}

	for index, events := range esIndices {
		err := es.checkIndex(index)
		if err != nil {
			return errors.Wrap(err, "Error creating index in elasticsearch")
		}

		bulkReq := es.esClient.Bulk().Index(index)
		for _, event := range events {
			req := elastic.NewBulkIndexRequest().
				Type("event").
				Id(event.EventID).
				Doc(event)
			bulkReq = bulkReq.Add(req)
		}
		ctx := context.Background()
		bulkResp, err := bulkReq.Do(ctx)
		if err != nil {
			esMeter := metrics.GetOrRegisterMeter("esWriteError", es.registry)
			esMeter.Mark(1)
			return errors.Wrap(err, "Error performing bulk index in ES")
		}
		if bulkResp.Errors {
			// TODO: figure out what to do with failed event writes
			failedItems := bulkResp.Failed()
			for _, item := range failedItems {
				fmt.Println("failed to index event with id", item.Id)
			}
		}
	}

	topicArr := make([]string, 0)
	for tId, _ := range topicIDs {
		topicArr = append(topicArr, tId)
	}

	if err := es.cqlSession.Query(fmt.Sprintf("DELETE FROM temp_event WHERE event_id in (%s) AND topic_id in (%s)",
		strings.Join(eventIDs, ","), strings.Join(topicArr, ","))).Exec(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraWriteError", es.registry)
		cassMeter.Mark(1)
		return errors.Wrap(err, "Error performing delete from temp_event in cassandra")
	}
	return nil
}

func (es *EventStore) CloseSession() {
	es.cqlSession.Close()
}
