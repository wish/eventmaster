package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ContextLogic/eventmaster/eventmaster"
	"github.com/ContextLogic/goServiceLookup/servicelookup"
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
			property, ok := newP[prop]
			if !ok {
				return false
			}
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
			oldInnerP, ok := oldP[k]
			if !ok {
				continue
			}
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

type UnaddedEvent struct {
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
		stringifyArr(event.Tags), stringify(data), event.ReceivedTime*1000)
}

type TopicData struct {
	Name   string                 `json:"topic_name"`
	Schema map[string]interface{} `json:"data_schema"`
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

func createTables(session *gocql.Session) error {
	// setup up event, topic, and dc tables in Cassandra if they don't exist
	if err := session.Query(`
		CREATE TABLE IF NOT EXISTS event (
			event_id UUID,
			parent_event_id UUID,
			dc_id UUID,
			topic_id UUID,
			host text,
			target_host_set set<text>,
			user text,
			event_time timestamp,
			tag_set set<text>,
			data_json text,
			received_time timestamp,
			PRIMARY KEY ((event_id,topic_id),event_time)
		);`).Exec(); err != nil {
		return errors.Wrap(err, "Error creating event table in cassandra")
	}

	if err := session.Query(`
		CREATE TABLE IF NOT EXISTS temp_event (
			event_id UUID,
			parent_event_id UUID,
			dc_id UUID,
			topic_id UUID,
			host text,
			target_host_set set<text>,
			user text,
			event_time timestamp,
			tag_set set<text>,
			data_json text,
			received_time timestamp,
			PRIMARY KEY ((event_id,topic_id),event_time)
		);`).Exec(); err != nil {
		return errors.Wrap(err, "Error creating temp_event table in cassandra")
	}

	if err := session.Query(`
		CREATE TABLE IF NOT EXISTS event_topic (
			topic_id UUID,
			topic_name text,
			data_schema text,
			PRIMARY KEY (topic_id)
		);`).Exec(); err != nil {
		return errors.Wrap(err, "Error creating event_topic table in cassandra")
	}

	if err := session.Query(`
		CREATE TABLE IF NOT EXISTS event_dc (
			dc_id UUID,
			dc text,
			PRIMARY KEY (dc_id)
		);`).Exec(); err != nil {
		return errors.Wrap(err, "Error creating dc table in cassandra")
	}
	return nil
}

func NewEventStore(dbConf dbConfig, config Config, registry metrics.Registry) (*EventStore, error) {
	var cassandraIps, esIps []string

	if config.CassandraServiceName != "" {
		slClient := servicelookup.NewClient(false)
		cassandraIps = slClient.GetServiceIps(config.CassandraServiceName, "")
		esIps = slClient.GetServiceIps(config.ESServiceName, "")

		for i, ip := range esIps {
			if !strings.HasPrefix(ip, "http://") {
				esIps[i] = "http://" + ip
			}
			esIps[i] = fmt.Sprintf("%s:%s", esIps[i], config.CassandraPort)
		}
	} else {
		cassandraAddr := "127.0.0.1:9042"
		esAddr := "http://127.0.0.1:9200"
		if dbConf.CassandraAddr != "" {
			cassandraAddr = dbConf.CassandraAddr
		}
		if dbConf.ESAddr != "" {
			esAddr = dbConf.ESAddr
		}
		cassandraIps = append(cassandraIps, cassandraAddr)
		esIps = append(esIps, esAddr)
	}

	fmt.Println("Connecting to Cassandra:", cassandraIps)

	// Establish connection to Cassandra
	cluster := gocql.NewCluster(cassandraIps...)
	cluster.Keyspace = dbConf.Keyspace
	if dbConf.Consistency == "one" {
		cluster.Consistency = gocql.One
	} else if dbConf.Consistency == "two" {
		cluster.Consistency = gocql.Two
	} else if dbConf.Consistency == "quorum" {
		cluster.Consistency = gocql.Quorum
	} else {
		cluster.Consistency = gocql.LocalQuorum
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, errors.Wrap(err, "Error creating cassandra session")
	}

	if err := createTables(session); err != nil {
		return nil, errors.Wrap(err, "Error creating tables in cassandra")
	}

	fmt.Println("Connecting to ES:", esIps)

	esOpts := []elastic.ClientOptionFunc{elastic.SetURL(esIps...)}
	if dbConf.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(dbConf.CertFile, dbConf.KeyFile)
		if err != nil {
			return nil, errors.Wrap(err, "Error loading x509 key pair")
		}
		caCert, err := ioutil.ReadFile(dbConf.CAFile)
		if err != nil {
			return nil, errors.Wrap(err, "Error loading CA file")
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		// Setup HTTPS client
		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
		tlsConfig.BuildNameToCertificate()
		transport := &http.Transport{TLSClientConfig: tlsConfig}
		httpClient := &http.Client{Transport: transport}

		esOpts = append(esOpts, elastic.SetHttpClient(httpClient))
	} else {
		esOpts = append(esOpts, elastic.SetBasicAuth(dbConf.ESUsername, dbConf.ESPassword))
	}

	client, err := elastic.NewClient(esOpts...)
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
				dcs = append(dcs, strings.Replace(id, "-", "_", -1))
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
				topics = append(topics, strings.Replace(id, "-", "_", -1))
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
	if len(q.ParentEventId) != 0 {
		parentEventIds := make([]interface{}, 0)
		for _, id := range q.ParentEventId {
			parentEventIds = append(parentEventIds, strings.Replace(id, "-", "_", -1))
		}
		queries = append(queries, elastic.NewTermsQuery("parent_event_id", parentEventIds...))
	}
	if len(q.User) != 0 {
		users := make([]interface{}, 0)
		for _, user := range q.User {
			users = append(users, user)
		}
		queries = append(queries, elastic.NewTermsQuery("user", users...))
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
		if q.StartEventTime != 0 {
			rq.Gte(q.StartEventTime)
		}
		if q.EndEventTime != 0 {
			rq.Lte(q.EndEventTime)
		}
		query = query.Must(rq)
	}

	if q.StartReceivedTime != 0 || q.EndReceivedTime != 0 {
		rq := elastic.NewRangeQuery("received_time")
		if q.StartReceivedTime != 0 {
			rq.Gte(q.StartReceivedTime)
		}
		if q.EndReceivedTime != 0 {
			rq.Lte(q.EndReceivedTime)
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

	return &Event{
		EventID:       uuid.NewV4().String(),
		ParentEventID: event.ParentEventID,
		EventTime:     event.EventTime,
		DcID:          dcID,
		TopicID:       topicID,
		Tags:          event.Tags,
		Host:          event.Host,
		TargetHosts:   event.TargetHosts,
		User:          event.User,
		Data:          event.Data,
		ReceivedTime:  time.Now().Unix(),
	}, data, nil
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

	sortByTime := false
	for i, field := range q.SortField {
		if field == "dc" {
			sq.Sort("dc_id.keyword", q.SortAscending[i])
		} else if field == "topic" {
			sq.Sort("topic_id.keyword", q.SortAscending[i])
		} else {
			if field == "event_time" {
				sortByTime = true
			}
			sq.Sort(fmt.Sprintf("%s.%s", field, "keyword"), q.SortAscending[i])
		}
	}

	if !sortByTime {
		sq.Sort("event_time", false)
	}

	sr, err := sq.Do(ctx)
	if err != nil {
		esMeter := metrics.GetOrRegisterMeter("esSearchError", es.registry)
		esMeter.Mark(1)
		return nil, errors.Wrap(err, "Error executing ES search query")

	}

	schemas := make(map[string](map[string]interface{}))
	es.topicMutex.Lock()
	for k, v := range es.topicSchemaPropertiesMap {
		schemas[k] = v
	}
	es.topicMutex.Unlock()

	var evt Event
	for _, item := range sr.Each(reflect.TypeOf(evt)) {
		if e, ok := item.(Event); ok {
			e.TopicID = strings.Replace(e.TopicID, "_", "-", -1)
			e.DcID = strings.Replace(e.DcID, "_", "-", -1)
			e.ParentEventID = strings.Replace(e.ParentEventID, "_", "-", -1)
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
	timer := metrics.GetOrRegisterTimer("AddEvent", es.registry)
	defer timer.UpdateSince(start)

	evt, data, err := es.augmentEvent(event)
	if err != nil {
		return "", errors.Wrap(err, "Error augmenting event")
	}

	// write event to Cassandra
	queryStr := evt.toCassandra(data)
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
			var s map[string]interface{}
			err := json.Unmarshal([]byte(schema), &s)
			if err != nil {
				return nil, errors.Wrap(err, "Error unmarshalling schema")
			}
			topics = append(topics, TopicData{
				Name:   name,
				Schema: s,
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

func (es *EventStore) AddTopic(topic TopicData) (string, error) {
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("AddTopic", es.registry)
	defer timer.UpdateSince(start)

	name := topic.Name
	schema := topic.Schema

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
	queryStr := fmt.Sprintf(`
    INSERT INTO event_topic (topic_id, topic_name, data_schema)
    VALUES (%[1]s, %[2]s, %[3]s);`,
		id, stringify(name), stringify(schemaStr))
	query := es.cqlSession.Query(queryStr)

	if err := query.Exec(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraWriteError", es.registry)
		cassMeter.Mark(1)
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
	timer := metrics.GetOrRegisterTimer("UpdateTopic", es.registry)
	defer timer.UpdateSince(start)

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
	queryStr := fmt.Sprintf(`UPDATE event_topic 
		SET topic_name = %s`, stringify(newName))

	var jsonSchema *gojsonschema.Schema
	var ok bool
	schemaStr := "{}"
	if schema != nil {
		schemaBytes, err := json.Marshal(schema)
		if err != nil {
			return "", errors.Wrap(err, "Error marshalling schema into json")
		}
		schemaStr = string(schemaBytes)
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
	es.topicSchemaPropertiesMap[id] = schema
	es.topicMutex.Unlock()

	fmt.Println("Topic Updated:", newName, id)
	return id, nil
}

func (es *EventStore) DeleteTopic(deleteReq *eventmaster.DeleteTopicRequest) error {
	topicName := strings.ToLower(deleteReq.TopicName)
	id := es.getTopicId(topicName)
	if id == "" {
		return errors.New("Couldn't find topic id for topic:" + topicName)
	}
	if _, err := es.esClient.DeleteByQuery(es.getESIndices()...).
		Query(elastic.NewTermQuery("topic_id", strings.Replace(id, "-", "_", -1))).
		Do(context.Background()); err != nil {
		return errors.Wrap(err, "Error deleting events under topic from ES")
	}

	if err := es.cqlSession.Query(fmt.Sprintf(`
		DELETE FROM event_topic WHERE topic_id=%[1]s;`,
		id)).Exec(); err != nil {
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
	timer := metrics.GetOrRegisterTimer("AddDc", es.registry)
	defer timer.UpdateSince(start)

	name := strings.ToLower(dc.Dc)
	if name == "" {
		return "", errors.New("Error adding dc - dc name is empty")
	}
	id := es.getDcId(name)
	if id != "" {
		return "", errors.New(fmt.Sprintf("Error adding dc - dc with name %s already exists", dc))
	}

	id = uuid.NewV4().String()
	queryStr := fmt.Sprintf(`
    INSERT INTO event_dc (dc_id, dc)
    VALUES (%[1]s, %[2]s);`,
		id, stringify(name))
	query := es.cqlSession.Query(queryStr)

	if err := query.Exec(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraWriteError", es.registry)
		cassMeter.Mark(1)
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
	timer := metrics.GetOrRegisterTimer("UpdateDc", es.registry)
	defer timer.UpdateSince(start)

	oldName := updateReq.OldName
	newName := updateReq.NewName

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

	fmt.Println("Dc Updated:", newName, id)
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
			id := dcId.String()
			newDcNameToId[dcName] = id
			newDcIdToName[id] = dcName
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
	eventIDs := make(map[string]struct{}) // make eventIDs a set to facilitate deletion in event of bulk error
	topicIDs := make(map[string]struct{})
	esIndices := make(map[string]([]*Event))
	var v struct{}

	for true {
		if iter.Scan(&eventID, &parentEventID, &dcID, &topicID, &host,
			&targetHostSet, &user, &eventTime, &tagSet, &data, &receivedTime) {
			var d map[string]interface{}
			if data != "" {
				if err := json.Unmarshal([]byte(data), &d); err != nil {
					return errors.Wrap(err, "Error unmarshalling JSON in event data")
				}
			}
			eventIDs[eventID.String()] = v
			topicIDs[topicID.String()] = v
			parentEventIDStr := parentEventID.String()
			if parentEventIDStr == "00000000-0000-0000-0000-000000000000" {
				parentEventIDStr = ""
			}
			index := getIndexFromTime(eventTime / 1000)
			esIndices[index] = append(esIndices[index], &Event{
				EventID:       eventID.String(),
				ParentEventID: strings.Replace(parentEventIDStr, "-", "_", -1),
				EventTime:     eventTime / 1000,
				DcID:          strings.Replace(dcID.String(), "-", "_", -1),
				TopicID:       strings.Replace(topicID.String(), "-", "_", -1),
				Tags:          tagSet,
				Host:          host,
				TargetHosts:   targetHostSet,
				User:          user,
				Data:          d,
				ReceivedTime:  receivedTime / 1000,
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
			failedItems := bulkResp.Failed()
			for _, item := range failedItems {
				fmt.Println("failed to index event with id", item.Id)
				delete(eventIDs, item.Id) // don't include event in list of events to de
			}
		}
	}

	var idArr []string
	for eventId, _ := range eventIDs {
		idArr = append(idArr, eventId)
	}

	var topicArr []string
	for tId, _ := range topicIDs {
		topicArr = append(topicArr, tId)
	}

	if err := es.cqlSession.Query(fmt.Sprintf("DELETE FROM temp_event WHERE event_id in (%s) AND topic_id in (%s)",
		strings.Join(idArr, ","), strings.Join(topicArr, ","))).Exec(); err != nil {
		cassMeter := metrics.GetOrRegisterMeter("cassandraWriteError", es.registry)
		cassMeter.Mark(1)
		return errors.Wrap(err, "Error performing delete from temp_event in cassandra")
	}
	return nil
}

func (es *EventStore) CloseSession() {
	es.cqlSession.Close()
}
