package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ContextLogic/eventmaster/eventmaster"
	cass "github.com/ContextLogic/eventmaster/src/cassandra_client"
	"github.com/ContextLogic/goServiceLookup/servicelookup"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/segmentio/ksuid"
	"github.com/xeipuuv/gojsonschema"
	elastic "gopkg.in/olivere/elastic.v5"
)

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

type UnaddedEvent struct {
	ParentEventID string                 `json:"parent_event_id"`
	EventTime     int64                  `json:"event_time"`
	Dc            string                 `json:"dc"`
	TopicName     string                 `json:"topic_name"`
	Tags          []string               `json:"tag_set"`
	Host          string                 `json:"host"`
	TargetHosts   []string               `json:"target_host_set"`
	User          string                 `json:"user"`
	Data          map[string]interface{} `json:"data"`
}

func (event *Event) toCassandra(data string) string {
	// TODO: change this to prevent tombstones
	return fmt.Sprintf(`
	BEGIN BATCH
    INSERT INTO event (event_id, parent_event_id, dc_id, topic_id, host, target_host_set, user, event_time, tag_set, data_json, received_time)
    VALUES (%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]s, %[7]s, %[8]d, %[9]s, $$%[10]s$$, %[11]d);
    INSERT INTO temp_event (event_id, parent_event_id, dc_id, topic_id, host, target_host_set, user, event_time, tag_set, data_json, received_time)
    VALUES (%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]s, %[7]s, %[8]d, %[9]s, $$%[10]s$$, %[11]d);
    APPLY BATCH;`,
		stringify(event.EventID), stringify(event.ParentEventID), stringifyUUID(event.DcID), stringifyUUID(event.TopicID),
		stringify(event.Host), stringifyArr(event.TargetHosts), stringify(event.User), event.EventTime,
		stringifyArr(event.Tags), data, event.ReceivedTime)
}

type TopicData struct {
	Id     string                 `json:"topic_id"`
	Name   string                 `json:"topic_name"`
	Schema map[string]interface{} `json:"data_schema"`
}

type DcData struct {
	Id   string `json:"dc_id"`
	Name string `json:"dc_name"`
}

type EventStore struct {
	cqlSession               cass.Session
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
	rander                   io.Reader // used to generate KSUID for event ID
	randMutex                *sync.Mutex
}

func NewEventStore(dbConf dbConfig, config Config) (*EventStore, error) {
	var cassandraIps, esIps []string

	slClient := servicelookup.NewClient(false)
	if config.CassandraServiceName != "" {
		cassandraIps = slClient.GetServiceIps(config.CassandraServiceName, "")
	} else {
		cassandraIps = dbConf.CassandraAddr
	}

	if config.ESServiceName != "" {
		esIps = slClient.GetServiceIps(config.ESServiceName, "")
		for i, ip := range esIps {
			if !strings.HasPrefix(ip, "http://") {
				esIps[i] = "http://" + ip
			}
			esIps[i] = fmt.Sprintf("%s:%s", esIps[i], config.CassandraPort)
		}
	} else {
		esIps = dbConf.ESAddr
	}

	fmt.Println("Connecting to Cassandra:", cassandraIps)

	// Establish connection to Cassandra
	session, err := cass.NewCqlSession(cassandraIps, dbConf.Keyspace, dbConf.Consistency)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating cassandra session")
	}

	fmt.Println("Connecting to ES:", esIps)

	esOpts := []elastic.ClientOptionFunc{elastic.SetURL(esIps...)}
	esOpts = append(esOpts, elastic.SetBasicAuth(dbConf.ESUsername, dbConf.ESPassword))

	client, err := elastic.NewClient(esOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating elasticsearch client")
	}

	return &EventStore{
		cqlSession:               session,
		esClient:                 client,
		topicMutex:               &sync.RWMutex{},
		dcMutex:                  &sync.RWMutex{},
		indexMutex:               &sync.RWMutex{},
		topicNameToId:            make(map[string]string),
		topicIdToName:            make(map[string]string),
		topicSchemaMap:           make(map[string]*gojsonschema.Schema),
		topicSchemaPropertiesMap: make(map[string](map[string]interface{})),
		dcNameToId:               make(map[string]string),
		dcIdToName:               make(map[string]string),
		rander:                   rand.Reader,
		randMutex:                &sync.Mutex{},
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

func (es *EventStore) getESIndices(startEventTime int64, endEventTime int64, topicIds ...string) []string {
	es.indexMutex.RLock()
	names := es.indexNames
	es.indexMutex.RUnlock()

	if startEventTime == -1 && endEventTime == -1 && len(topicIds) == 0 {
		return names
	}

	var filteredIndices []string
	if len(topicIds) > 0 {
		// TODO: make more efficient
		for _, index := range names {
			for _, id := range topicIds {
				if strings.HasPrefix(index, id) {
					filteredIndices = append(filteredIndices, index)
					break
				}
			}
		}
	} else {
		filteredIndices = names
	}

	if startEventTime != -1 {
		startIndex := getIndex("", startEventTime)
		for i := len(filteredIndices) - 1; i >= 0; i-- {
			curIndex := filteredIndices[i]
			if strings.Compare(string(curIndex[len(curIndex)-11:]), startIndex) < 0 {
				filteredIndices[i] = filteredIndices[len(filteredIndices)-1]
				filteredIndices = filteredIndices[:len(filteredIndices)-1]
			}
		}
	}
	if endEventTime != -1 {
		endIndex := getIndex("", endEventTime)
		for i := len(filteredIndices) - 1; i >= 0; i-- {
			curIndex := filteredIndices[i]
			if strings.Compare(string(curIndex[len(curIndex)-11:]), endIndex) > 0 {
				filteredIndices[i] = filteredIndices[len(filteredIndices)-1]
				filteredIndices = filteredIndices[:len(filteredIndices)-1]
			}
		}
	}
	return filteredIndices
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
	if q.EventId != "" {
		return elastic.NewIdsQuery().Ids(q.EventId)
	}

	var queries []elastic.Query

	if len(q.Dc) != 0 {
		var ids []string
		for _, dc := range q.Dc {
			if id := es.getDcId(strings.ToLower(dc)); id != "" {
				ids = append(ids, id)
			}
		}
		queries = append(queries, elastic.NewQueryStringQuery(fmt.Sprintf("%s:%s", "dc_id", "("+strings.Join(ids, " OR ")+")")))
	}
	if len(q.Host) != 0 {
		var hosts []string
		for _, host := range q.Host {
			hosts = append(hosts, strings.ToLower(host))
		}
		queries = append(queries, elastic.NewQueryStringQuery(fmt.Sprintf("%s:%s", "host", "("+strings.Join(hosts, " OR ")+")")))
	}
	if len(q.TargetHostSet) != 0 {
		thosts := make([]interface{}, 0)
		for _, host := range q.TargetHostSet {
			thosts = append(thosts, strings.ToLower(host))
		}
		queries = append(queries, elastic.NewTermsQuery("target_host_set", thosts...))
	}
	if len(q.TagSet) != 0 {
		tags := make([]interface{}, 0)
		for _, tag := range q.TagSet {
			tags = append(tags, strings.ToLower(tag))
		}
		queries = append(queries, elastic.NewTermsQuery("tag_set", tags...))
	}
	if len(q.ParentEventId) != 0 {
		var ids []string
		for _, id := range q.ParentEventId {
			ids = append(ids, strings.ToLower(id))
		}
		queries = append(queries, elastic.NewQueryStringQuery(fmt.Sprintf("%s:%s", "parent_event_id", "("+strings.Join(ids, " OR ")+")")))
	}
	if len(q.User) != 0 {
		var users []string
		for _, user := range q.User {
			users = append(users, strings.ToLower(user))
		}
		queries = append(queries, elastic.NewQueryStringQuery(fmt.Sprintf("%s:%s", "user", "("+strings.Join(users, " OR ")+")")))
	}
	if q.Data != "" {
		var d map[string]interface{}
		if err := json.Unmarshal([]byte(q.Data), &d); err != nil {
			queries = append(queries, elastic.NewQueryStringQuery(q.Data))
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
		if q.StartEventTime != 0 {
			rq.Gte(q.StartEventTime * 1000)
		}
		if q.EndEventTime != 0 {
			rq.Lte(q.EndEventTime * 1000)
		}
		query = query.Must(rq)
	}

	if q.StartReceivedTime != 0 || q.EndReceivedTime != 0 {
		rq := elastic.NewRangeQuery("received_time")
		if q.StartReceivedTime != 0 {
			rq.Gte(q.StartReceivedTime * 1000)
		}
		if q.EndReceivedTime != 0 {
			rq.Lte(q.EndReceivedTime * 1000)
		}
		query = query.Must(rq)
	}

	return query
}

func (es *EventStore) getSearchService(q *eventmaster.Query) *elastic.SearchService {
	query := es.buildESQuery(q)
	var ids []string
	for _, name := range q.TopicName {
		ids = append(ids, es.getTopicId(name))
	}

	if q.StartEventTime == 0 {
		q.StartEventTime = -1
	}
	if q.EndEventTime == 0 {
		q.EndEventTime = -1
	}
	indexNames := es.getESIndices(q.StartEventTime, q.EndEventTime, ids...)

	limit := q.Limit
	if limit == 0 {
		limit = 100
	}

	sq := es.esClient.Search().
		Index(indexNames...).
		Query(query).
		From(int(q.Start)).Size(int(limit)).
		Pretty(true)

	sortByTime := false
	for i, field := range q.SortField {
		if field == "dc" {
			sq.Sort("dc_id.keyword", q.SortAscending[i])
		} else if field == "topic" {
			sq.Sort("topic_id.keyword", q.SortAscending[i])
		} else if field == "event_time" {
			sortByTime = true
			sq.Sort("event_time", true)
		} else {
			sq.Sort(fmt.Sprintf("%s.%s", field, "keyword"), q.SortAscending[i])
		}
	}

	if !sortByTime {
		sq.Sort("event_time", false)
	}

	return sq
}

func (es *EventStore) insertDefaults(s map[string]interface{}, m map[string]interface{}) {
	properties := s["properties"]
	p, _ := properties.(map[string]interface{})
	insertDefaults(p, m)
}

func (es *EventStore) buildEventID(t int64) (string, error) {
	randBuffer := [16]byte{}
	es.randMutex.Lock()
	_, err := io.ReadAtLeast(es.rander, randBuffer[:], len(randBuffer))
	es.randMutex.Unlock()
	if err != nil {
		return "", errors.Wrap(err, "Error reading from rand.Reader")
	}

	id, err := ksuid.FromParts(time.Unix(t, 0).UTC(), randBuffer[:])
	if err != nil {
		return "", errors.Wrap(err, "Error creating ksuid")
	}
	return id.String(), nil
}

func (es *EventStore) augmentEvent(event *UnaddedEvent) (*Event, string, error) {
	// validate Event
	if event.Dc == "" {
		return nil, "", errors.New("Event missing dc")
	} else if event.Host == "" {
		return nil, "", errors.New("Event missing host")
	} else if event.TopicName == "" {
		return nil, "", errors.New("Event missing topic_name")
	}

	if event.EventTime == 0 {
		event.EventTime = time.Now().Unix()
	}

	dcID := es.getDcId(strings.ToLower(event.Dc))
	if dcID == "" {
		return nil, "", errors.New(fmt.Sprintf("Dc '%s' does not exist in dc table", strings.ToLower(event.Dc)))
	}
	topicID := es.getTopicId(strings.ToLower(event.TopicName))
	if topicID == "" {
		return nil, "", errors.New(fmt.Sprintf("Topic '%s' does not exist in topic table", strings.ToLower(event.TopicName)))
	}
	topicSchema := es.getTopicSchema(topicID)
	data := "{}"
	if topicSchema != nil {
		propertiesSchema := es.getTopicSchemaProperties(topicID)
		if event.Data == nil {
			event.Data = make(map[string]interface{})
		}
		es.insertDefaults(propertiesSchema, event.Data)
		dataBytes, err := json.Marshal(event.Data)
		if err != nil {
			return nil, "", errors.Wrap(err, "Error marshalling data with defaults into json")
		}
		data = string(dataBytes)
		dataLoader := gojsonschema.NewStringLoader(data)
		result, err := topicSchema.Validate(dataLoader)
		if err != nil {
			return nil, "", errors.Wrap(err, "Error validating event data against schema")
		}
		if !result.Valid() {
			errMsg := ""
			for _, err := range result.Errors() {
				errMsg = fmt.Sprintf("%s, %s", errMsg, err)
			}
			return nil, "", errors.New(errMsg)
		}
	}

	eventID, err := es.buildEventID(event.EventTime)
	if err != nil {
		return nil, "", errors.Wrap(err, "Error creating event ID:")
	}

	return &Event{
		EventID:       eventID,
		ParentEventID: event.ParentEventID,
		EventTime:     event.EventTime * 1000,
		DcID:          dcID,
		TopicID:       topicID,
		Tags:          event.Tags,
		Host:          event.Host,
		TargetHosts:   event.TargetHosts,
		User:          event.User,
		Data:          event.Data,
		ReceivedTime:  time.Now().Unix() * 1000,
	}, data, nil
}

func (es *EventStore) checkIndex(index string) error {
	ctx := context.Background()
	exists, err := es.esClient.IndexExists(index).Do(ctx)
	if err != nil {
		return errors.Wrap(err, "Error finding if index exists in ES")
	}
	if !exists {
		if _, err := es.esClient.CreateIndex(index).Do(ctx); err != nil {
			return errors.Wrap(err, "Error creating event_master index in ES")
		}
		if _, err := es.esClient.
			PutMapping().
			Index(index).
			Type("event").
			BodyString(EventTypeMapping).
			Do(ctx); err != nil {
			return errors.Wrap(err, "Error creating event mapping in ES")
		}
	}
	return nil
}

func (es *EventStore) Find(q *eventmaster.Query) ([]*Event, error) {
	start := time.Now()
	defer func() {
		eventStoreTimer.WithLabelValues("Find").Observe(trackTime(start))
	}()

	sq := es.getSearchService(q)
	ctx := context.Background()

	sr, err := sq.Do(ctx)
	if err != nil {
		eventStoreDbErrCounter.WithLabelValues("es", "read").Inc()
		return nil, errors.Wrap(err, "Error executing ES search query")
	}

	schemas := make(map[string](map[string]interface{}))
	es.topicMutex.Lock()
	for k, v := range es.topicSchemaPropertiesMap {
		schemas[k] = v
	}
	es.topicMutex.Unlock()

	var evts []*Event
	var evt Event
	for _, item := range sr.Each(reflect.TypeOf(evt)) {
		if e, ok := item.(Event); ok {
			e.EventTime /= 1000
			topicID := e.TopicID
			propertiesSchema := schemas[topicID]
			if propertiesSchema != nil {
				es.insertDefaults(propertiesSchema, e.Data)
			}
			evts = append(evts, &e)
		}
	}

	return evts, nil
}

func (es *EventStore) AddEvent(event *UnaddedEvent) (string, error) {
	start := time.Now()
	defer func() {
		eventStoreTimer.WithLabelValues("AddEvent").Observe(trackTime(start))
	}()

	evt, data, err := es.augmentEvent(event)
	if err != nil {
		return "", errors.Wrap(err, "Error augmenting event")
	}

	// write event to Cassandra
	queryStr := evt.toCassandra(data)
	if err := es.cqlSession.ExecQuery(queryStr); err != nil {
		eventStoreDbErrCounter.WithLabelValues("cassandra", "write").Inc()
		return "", errors.Wrap(err, "Error executing insert query in Cassandra")
	}
	fmt.Println("Event added:", evt.EventID)
	return evt.EventID, nil
}

func (es *EventStore) GetTopics() ([]TopicData, error) {
	start := time.Now()
	defer func() {
		eventStoreTimer.WithLabelValues("GetTopics").Observe(trackTime(start))
	}()

	scanIter, closeIter := es.cqlSession.ExecIterQuery("SELECT topic_id, topic_name, data_schema FROM event_topic;")
	var topicId gocql.UUID
	var name, schema string
	var topics []TopicData
	for {
		if scanIter(&topicId, &name, &schema) {
			var s map[string]interface{}
			err := json.Unmarshal([]byte(schema), &s)
			if err != nil {
				return nil, errors.Wrap(err, "Error unmarshalling schema")
			}
			topics = append(topics, TopicData{
				Id:     topicId.String(),
				Name:   name,
				Schema: s,
			})
		} else {
			break
		}
	}
	if err := closeIter(); err != nil {
		eventStoreDbErrCounter.WithLabelValues("cassandra", "read").Inc()
		return nil, errors.Wrap(err, "Error closing iter")
	}
	return topics, nil
}

func (es *EventStore) GetDcs() ([]DcData, error) {
	start := time.Now()
	defer func() {
		eventStoreTimer.WithLabelValues("GetDcs").Observe(trackTime(start))
	}()

	scanIter, closeIter := es.cqlSession.ExecIterQuery("SELECT dc_id, dc FROM event_dc;")
	var id gocql.UUID
	var dc string
	var dcs []DcData
	for true {
		if scanIter(&id, &dc) {
			dcs = append(dcs, DcData{
				Id:   id.String(),
				Name: dc,
			})
		} else {
			break
		}
	}
	if err := closeIter(); err != nil {
		eventStoreDbErrCounter.WithLabelValues("cassandra", "read").Inc()
		return nil, errors.Wrap(err, "Error closing iter")
	}
	return dcs, nil
}

func (es *EventStore) AddTopic(topic TopicData) (string, error) {
	start := time.Now()
	defer func() {
		eventStoreTimer.WithLabelValues("AddTopic").Observe(trackTime(start))
	}()

	name := topic.Name
	schema := topic.Schema

	if name == "" {
		return "", errors.New("Topic name cannot be empty")
	} else if es.getTopicId(name) != "" {
		return "", errors.New("Topic with name already exists")
	}

	schemaStr := "{}"
	if schema != nil {
		schemaBytes, err := json.Marshal(schema)
		if err != nil {
			return "", errors.Wrap(err, "Error marshalling schema into json")
		}
		schemaStr = string(schemaBytes)
	}

	jsonSchema, ok := es.validateSchema(schemaStr)
	if !ok {
		return "", errors.New("Error adding topic - schema is not in valid JSON format")
	}

	id := uuid.NewV4().String()
	queryStr := fmt.Sprintf(`INSERT INTO event_topic (topic_id, topic_name, data_schema)
		VALUES (%[1]s, %[2]s, %[3]s);`,
		id, stringify(name), stringify(schemaStr))

	if err := es.cqlSession.ExecQuery(queryStr); err != nil {
		eventStoreDbErrCounter.WithLabelValues("cassandra", "write").Inc()
		return "", errors.Wrap(err, "Error executing insert query in Cassandra")
	}

	es.topicMutex.Lock()
	es.topicNameToId[name] = id
	es.topicIdToName[id] = name
	es.topicSchemaPropertiesMap[id] = schema
	es.topicSchemaMap[id] = jsonSchema
	es.topicMutex.Unlock()

	fmt.Println("Topic Added:", name, id)
	return id, nil
}

func (es *EventStore) UpdateTopic(oldName string, td TopicData) (string, error) {
	start := time.Now()
	defer func() {
		eventStoreTimer.WithLabelValues("UpdateTopic").Observe(trackTime(start))
	}()

	newName := td.Name
	schema := td.Schema

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
	queryStr := fmt.Sprintf(`UPDATE event_topic SET topic_name=%s`, stringify(newName))

	var jsonSchema *gojsonschema.Schema
	var ok bool
	if schema != nil {
		schemaBytes, err := json.Marshal(schema)
		if err != nil {
			return "", errors.Wrap(err, "Error marshalling schema into json")
		}
		schemaStr := string(schemaBytes)
		jsonSchema, ok = es.validateSchema(schemaStr)
		if !ok {
			return "", errors.New("Error adding topic - schema is not in valid JSON schema format")
		}

		old := es.getTopicSchemaProperties(id)
		ok = checkBackwardsCompatible(old, schema)
		if !ok {
			return "", errors.New("Error adding topic - new schema is not backwards compatible")
		}
		queryStr = fmt.Sprintf("%s, data_schema = %s", queryStr, stringify(schemaStr))
	}

	queryStr = fmt.Sprintf("%s WHERE topic_id=%s;", queryStr, id)
	if err := es.cqlSession.ExecQuery(queryStr); err != nil {
		eventStoreDbErrCounter.WithLabelValues("cassandra", "write").Inc()
		return "", errors.Wrap(err, "Error executing update query in Cassandra")
	}

	es.topicMutex.Lock()
	es.topicNameToId[newName] = es.topicNameToId[oldName]
	es.topicIdToName[id] = newName
	if newName != oldName {
		delete(es.topicNameToId, oldName)
	}
	es.topicSchemaMap[id] = jsonSchema
	es.topicSchemaPropertiesMap[id] = schema
	es.topicMutex.Unlock()

	fmt.Println("Topic Updated:", newName, id)
	return id, nil
}

func (es *EventStore) DeleteTopic(deleteReq *eventmaster.DeleteTopicRequest) error {
	start := time.Now()
	defer func() {
		eventStoreTimer.WithLabelValues("DeleteTopic").Observe(trackTime(start))
	}()

	topicName := strings.ToLower(deleteReq.TopicName)
	id := es.getTopicId(topicName)
	if id == "" {
		return errors.New("Couldn't find topic id for topic:" + topicName)
	}

	indices := es.getESIndices(-1, -1, id)
	if len(indices) > 0 {
		_, err := es.esClient.DeleteIndex(indices...).Do(context.Background())
		if err != nil {
			return errors.Wrap(err, "Error deleting events under topic from ES")
		}
	}

	if err := es.cqlSession.ExecQuery(fmt.Sprintf(`DELETE FROM event_topic WHERE topic_id=%[1]s;`,
		id)); err != nil {
		return errors.Wrap(err, "Error deleting topic and its events from cassandra")
	}

	es.topicMutex.Lock()
	delete(es.topicNameToId, topicName)
	delete(es.topicIdToName, id)
	delete(es.topicSchemaMap, id)
	delete(es.topicSchemaPropertiesMap, id)
	es.topicMutex.Unlock()

	fmt.Println("Topic Deleted:", topicName, id)
	return nil
}

func (es *EventStore) AddDc(dc *eventmaster.Dc) (string, error) {
	start := time.Now()
	defer func() {
		eventStoreTimer.WithLabelValues("AddDc").Observe(trackTime(start))
	}()

	name := strings.ToLower(dc.DcName)
	if name == "" {
		return "", errors.New("Error adding dc - dc name is empty")
	}
	id := es.getDcId(name)
	if id != "" {
		return "", errors.New(fmt.Sprintf("Error adding dc - dc with name %s already exists", dc))
	}

	id = uuid.NewV4().String()
	queryStr := fmt.Sprintf(`INSERT INTO event_dc (dc_id, dc) VALUES (%[1]s, %[2]s);`,
		id, stringify(name))

	if err := es.cqlSession.ExecQuery(queryStr); err != nil {
		eventStoreDbErrCounter.WithLabelValues("cassandra", "write").Inc()
		return "", errors.Wrap(err, "Error executing insert query in Cassandra")
	}

	es.dcMutex.Lock()
	es.dcIdToName[id] = name
	es.dcNameToId[name] = id
	es.dcMutex.Unlock()

	fmt.Println("Dc Added:", name, id)
	return id, nil
}

func (es *EventStore) UpdateDc(updateReq *eventmaster.UpdateDcRequest) (string, error) {
	start := time.Now()
	defer func() {
		eventStoreTimer.WithLabelValues("UpdateDc").Observe(trackTime(start))
	}()

	oldName := updateReq.OldName
	newName := updateReq.NewName

	if newName == "" {
		return "", errors.New("Dc name cannot be empty")
	}

	id := es.getDcId(newName)
	if oldName != newName && id != "" {
		return "", errors.New(fmt.Sprintf("Error updating dc - dc with name %s already exists", newName))
	}
	id = es.getDcId(oldName)
	if id == "" {
		return "", errors.New(fmt.Sprintf("Error updating dc - dc with name %s doesn't exist", oldName))
	}
	queryStr := fmt.Sprintf(`UPDATE event_dc SET dc=%s WHERE dc_id=%s;`,
		stringify(newName), id)

	if err := es.cqlSession.ExecQuery(queryStr); err != nil {
		eventStoreDbErrCounter.WithLabelValues("cassandra", "write").Inc()
		return "", errors.Wrap(err, "Error executing update query in Cassandra")
	}
	es.dcMutex.Lock()
	es.dcNameToId[newName] = es.dcNameToId[oldName]
	es.dcIdToName[id] = newName
	if newName != oldName {
		delete(es.dcNameToId, oldName)
	}
	es.dcMutex.Unlock()

	fmt.Println("Dc Updated:", newName, id)
	return id, nil
}

func (es *EventStore) Update() error {
	start := time.Now()
	defer func() {
		eventStoreTimer.WithLabelValues("Update").Observe(trackTime(start))
	}()

	newDcNameToId := make(map[string]string)
	newDcIdToName := make(map[string]string)
	scanIter, closeIter := es.cqlSession.ExecIterQuery("SELECT dc_id, dc FROM event_dc;")
	var dcId gocql.UUID
	var dcName string
	for true {
		if scanIter(&dcId, &dcName) {
			id := dcId.String()
			newDcNameToId[dcName] = id
			newDcIdToName[id] = dcName
		} else {
			break
		}
	}
	if err := closeIter(); err != nil {
		eventStoreDbErrCounter.WithLabelValues("cassandra", "read").Inc()
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

	scanIter, closeIter = es.cqlSession.ExecIterQuery("SELECT topic_id, topic_name, data_schema FROM event_topic;")
	var topicId gocql.UUID
	var topicName, dataSchema string
	for true {
		if scanIter(&topicId, &topicName, &dataSchema) {
			id := topicId.String()
			newTopicNameToId[topicName] = id
			newTopicIdToName[id] = topicName
			schemaMap[id] = dataSchema
		} else {
			break
		}
	}
	if err := closeIter(); err != nil {
		eventStoreDbErrCounter.WithLabelValues("cassandra", "read").Inc()
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
		eventStoreDbErrCounter.WithLabelValues("es", "read").Inc()
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
	defer func() {
		eventStoreTimer.WithLabelValues("FlushToES").Observe(trackTime(start))
	}()

	scanIter, closeIter := es.cqlSession.ExecIterQuery(`SELECT event_id, parent_event_id, dc_id, topic_id,
		host, target_host_set, user, event_time, tag_set, data_json, received_time
		FROM temp_event LIMIT 1000;`)
	var topicID, dcID gocql.UUID
	var eventTime, receivedTime int64
	var eventID, parentEventID, host, user, data string
	var targetHostSet, tagSet []string

	// keep track of event and topic IDs to generate delete query from cassandra
	eventIDs := make(map[string]struct{}) // make eventIDs a set to facilitate deletion in event of bulk error
	topicIDs := make(map[string]struct{})
	dcIDs := make(map[string]struct{})
	esIndices := make(map[string]([]*Event))
	var v struct{}

	for true {
		if scanIter(&eventID, &parentEventID, &dcID, &topicID, &host,
			&targetHostSet, &user, &eventTime, &tagSet, &data, &receivedTime) {
			var d map[string]interface{}
			if data != "" {
				if err := json.Unmarshal([]byte(data), &d); err != nil {
					return errors.Wrap(err, "Error unmarshalling JSON in event data")
				}
			}
			eventIDs[eventID] = v
			topicIDs[topicID.String()] = v
			dcIDs[dcID.String()] = v
			index := getIndex(topicID.String(), eventTime/1000)
			esIndices[index] = append(esIndices[index], &Event{
				EventID:       eventID,
				ParentEventID: parentEventID,
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
	if err := closeIter(); err != nil {
		eventStoreDbErrCounter.WithLabelValues("cassandra", "read").Inc()
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
			eventStoreDbErrCounter.WithLabelValues("es", "write").Inc()
			return errors.Wrap(err, "Error performing bulk index in ES")
		}
		if bulkResp.Errors {
			failedItems := bulkResp.Failed()
			for _, item := range failedItems {
				fmt.Println("failed to index event with id", item.Id)
				delete(eventIDs, item.Id) // don't include event in list of events to de
			}
		}
	}

	var idArr []string
	for eventId, _ := range eventIDs {
		idArr = append(idArr, fmt.Sprintf("'%s'", eventId))
	}

	var topicArr []string
	for tId, _ := range topicIDs {
		topicArr = append(topicArr, tId)
	}

	var dcArr []string
	for dcId, _ := range dcIDs {
		dcArr = append(dcArr, dcId)
	}

	if err := es.cqlSession.ExecQuery(fmt.Sprintf("DELETE FROM temp_event WHERE event_id in (%s) AND topic_id in (%s) AND dc_id in (%s)",
		strings.Join(idArr, ","), strings.Join(topicArr, ","), strings.Join(dcArr, ","))); err != nil {
		eventStoreDbErrCounter.WithLabelValues("cassandra", "write").Inc()
		return errors.Wrap(err, "Error performing delete from temp_event in cassandra")
	}
	return nil
}

func (es *EventStore) CloseSession() {
	es.cqlSession.Close()
}
