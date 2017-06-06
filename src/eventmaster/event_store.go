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
	ReceivedTime  int64                  `json:"received_time"`
}

type TopicData struct {
	Name   string `json:"topic_name"`
	Schema string `json:"data_schema"`
}

type DcData struct {
	Name string `json:"dc"`
}

type EventStore struct {
	cqlSession     *gocql.Session
	esClient       *elastic.Client
	topicNameToId  map[string]string               // map of name to id
	topicIdToName  map[string]string               // map of id to name
	topicSchemaMap map[string]*gojsonschema.Schema // map of topic id to json loader for schema validation
	dcNameToId     map[string]string               // map of name to id
	dcIdToName     map[string]string               // map of id to name
	indexNames     []string                        // list of name of all indices in es cluster
	topicMutex     *sync.RWMutex
	dcMutex        *sync.RWMutex
	indexMutex     *sync.RWMutex
	registry       metrics.Registry
}

func NewEventStore(dbConf dbConfig, registry metrics.Registry) (*EventStore, error) {
	// Establish connection to Cassandra
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
	client, err := elastic.NewClient(elastic.SetURL(dbConf.ESUrl))
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
	name := es.topicSchemaMap[id]
	es.topicMutex.RUnlock()
	return name
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

func (event *Event) toCassandra(dcID string, topicID string, data string) string {
	// TODO: change this to prevent tombstones
	return fmt.Sprintf(`
	BEGIN BATCH
    INSERT INTO event (event_id, parent_event_id, dc_id, topic_id, host, target_host_set, user, event_time, tag_set, data_json, received_time)
    VALUES (%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]s, %[7]s, %[8]d, %[9]s, %[10]s, %[11]d);
    INSERT INTO temp_event (event_id, parent_event_id, dc_id, topic_id, host, target_host_set, user, event_time, tag_set, data_json, received_time)
    VALUES (%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]s, %[7]s, %[8]d, %[9]s, %[10]s, %[11]d);
    APPLY BATCH;`,
		event.EventID, stringifyUUID(event.ParentEventID), dcID, topicID,
		stringify(event.Host), stringifyArr(event.TargetHosts), stringify(event.User), event.EventTime*1000,
		stringifyArr(event.Tags), stringify(data), event.ReceivedTime)
}

func (es *EventStore) validateSchema(schema string) bool {
	loader := gojsonschema.NewStringLoader(schema)
	_, err := gojsonschema.NewSchema(loader)
	if err != nil {
		return false
	}
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

func (es *EventStore) validateEvent(event *eventmaster.Event) (bool, string) {
	if event.Dc == "" {
		return false, "Event missing dc"
	} else if event.Host == "" {
		return false, "Event missing host"
	} else if event.TopicName == "" {
		return false, "Event missing topic_name"
	} else if event.EventTime == 0 {
		event.EventTime = time.Now().Unix()
	}

	id := es.getDcId(event.Dc)
	if id == "" {
		return false, fmt.Sprintf("Dc '%s' does not exist in dc table", strings.ToLower(event.Dc))
	}
	id = es.getTopicId(event.TopicName)
	if id == "" {
		return false, fmt.Sprintf("Topic '%s' does not exist in topic table", strings.ToLower(event.TopicName))
	}
	topicSchema := es.getTopicSchema(id)
	if topicSchema != nil {
		if event.Data == "" {
			event.Data = "{}"
		}
		dataLoader := gojsonschema.NewStringLoader(event.Data)
		result, err := topicSchema.Validate(dataLoader)
		if err != nil {
			return false, "Error validating event data against schema: " + err.Error()
		}
		if !result.Valid() {
			errMsg := ""
			for _, err := range result.Errors() {
				errMsg = fmt.Sprintf("%s, %s", errMsg, err)
			}
			return false, errMsg
		}
	}
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

	var evt Event
	var evts []*Event
	if len(indexNames) == 0 {
		return evts, nil
	}

	size := q.Size
	if size == 0 {
		size = 50
	}

	sq := es.esClient.Search().
		Index(indexNames...).
		Query(bq).
		From(int(q.From)).Size(int(size)).
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

	for _, item := range sr.Each(reflect.TypeOf(evt)) {
		if t, ok := item.(Event); ok {
			evts = append(evts, &t)
		}
	}
	return evts, nil
}

func (es *EventStore) AddEvent(event *eventmaster.Event) (string, error) {
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("AddEvent", es.registry)
	defer timer.UpdateSince(start)

	if ok, msg := es.validateEvent(event); !ok {
		return "", errors.New(msg)
	}
	evt, err := es.augmentEvent(event)
	if err != nil {
		return "", errors.Wrap(err, "Error augmenting event")
	}
	// write event to Cassandra
	queryStr := evt.toCassandra(es.getDcId(evt.Dc), es.getDcId(evt.TopicName), event.Data)
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

	ok := es.validateSchema(schema)
	if !ok {
		return "", errors.New("Error adding topic - schema is not in valid JSON format")
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
	return id, nil
}

func (es *EventStore) UpdateTopic(oldName string, newName string, schema string) (string, error) {
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("UpdateTopic", es.registry)
	defer timer.UpdateSince(start)

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
	if schema != "" {
		ok := es.validateSchema(schema)
		if !ok {
			return "", errors.New("Error adding topic - schema is not in valid JSON schema format")
		}
		queryStr = fmt.Sprintf("%s, data_schema = %s", queryStr, stringify(schema))
	}
	queryStr = fmt.Sprintf("%s WHERE topic_id = %s;", queryStr, id)
	if err := es.cqlSession.Query(queryStr).Exec(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraWriteError", es.registry)
		cassMeter.Mark(1)
		return "", errors.Wrap(err, "Error executing update query in Cassandra")
	}
	var jsonSchema *gojsonschema.Schema
	var err error
	if schema != "" {
		schemaLoader := gojsonschema.NewStringLoader(schema)
		jsonSchema, err = gojsonschema.NewSchema(schemaLoader)
		if err != nil {
			return "", errors.Wrap(err, "Schema is not in valid JSON format")
		}
	}
	es.topicMutex.Lock()
	es.topicNameToId[newName] = es.topicNameToId[oldName]
	es.topicIdToName[id] = newName
	if newName != oldName {
		delete(es.topicNameToId, oldName)
	}
	es.topicSchemaMap[id] = jsonSchema
	es.topicMutex.Unlock()
	return id, nil
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
	return id, nil
}

func (es *EventStore) UpdateDc(oldName string, newName string) (string, error) {
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("UpdateDc", es.registry)
	defer timer.UpdateSince(start)

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
			schemaLoader := gojsonschema.NewStringLoader(schema)
			jsonSchema, err := gojsonschema.NewSchema(schemaLoader)
			if err != nil {
				return errors.Wrap(err, "Error validating schema for topic "+id)
			}
			newTopicSchemaMap[id] = jsonSchema
		}
	}
	es.topicMutex.Lock()
	es.topicNameToId = newTopicNameToId
	es.topicIdToName = newTopicIdToName
	es.topicSchemaMap = newTopicSchemaMap
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
	eventIDs := make([]string, 0)
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
			dcName := es.getDcName(dcID.String())
			topicName := es.getTopicName(topicID.String())
			esIndices[index] = append(esIndices[index], &Event{
				EventID:       eventID.String(),
				ParentEventID: parentEventIDStr,
				EventTime:     eventTime,
				Dc:            dcName,
				TopicName:     topicName,
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
