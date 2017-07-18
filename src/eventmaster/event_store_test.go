package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/ContextLogic/eventmaster/eventmaster"
	cass "github.com/ContextLogic/eventmaster/src/cassandra_client"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/assert"
	"github.com/xeipuuv/gojsonschema"
	elastic "gopkg.in/olivere/elastic.v5"
)

var uuidMatchStr = "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-4[a-fA-F0-9]{3}-[8|9|aA|bB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}"

func isUUID(uuid string) bool {
	r := regexp.MustCompile(fmt.Sprintf("^%s$", uuidMatchStr))
	return r.MatchString(uuid)
}

var dataSchema = map[string]interface{}{
	"title":       "test",
	"description": "test",
	"type":        "object",
	"required":    []interface{}{"user_id"},
	"properties": map[string]interface{}{
		"first_name": map[string]interface{}{
			"type":    "string",
			"default": "bob",
		},
		"last_name": map[string]interface{}{
			"type": "string",
		},
		"user_id": map[string]interface{}{
			"type":    "integer",
			"minimum": 0,
		},
	},
}

var testTopics = []TopicData{
	TopicData{Name: "test1"},
	TopicData{Name: "test2"},
	TopicData{Name: "test3", Schema: dataSchema},
}

var testDcs = []*eventmaster.Dc{
	&eventmaster.Dc{DcName: "dc1"},
	&eventmaster.Dc{DcName: "dc2"},
	&eventmaster.Dc{DcName: "dc3"},
}

func populateTopics(s *EventStore) error {
	for _, topic := range testTopics {
		_, err := s.AddTopic(topic)
		if err != nil {
			return err
		}
	}
	return nil
}

func populateDcs(s *EventStore) error {
	for _, dc := range testDcs {
		_, err := s.AddDc(dc)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewMockESServer() *httptest.Server {
	handler := func(w http.ResponseWriter, r *http.Request) {
		resp := `{}`
		w.Write([]byte(resp))
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler(w, r)
	}))

	return ts
}

func GetTestEventStore(testESServer *httptest.Server) (*EventStore, error) {
	testESClient, err := elastic.NewSimpleClient(elastic.SetURL(testESServer.URL))
	if err != nil {
		return nil, err
	}

	return &EventStore{
		cqlSession:               &cass.MockCassSession{},
		esClient:                 testESClient,
		topicMutex:               &sync.RWMutex{},
		dcMutex:                  &sync.RWMutex{},
		indexMutex:               &sync.RWMutex{},
		topicNameToId:            make(map[string]string),
		topicIdToName:            make(map[string]string),
		topicSchemaMap:           make(map[string]*gojsonschema.Schema),
		topicSchemaPropertiesMap: make(map[string](map[string]interface{})),
		dcNameToId:               make(map[string]string),
		dcIdToName:               make(map[string]string),
		registry:                 metrics.NewRegistry(),
	}, nil
}

/******************************************
	TOPIC TESTS BEGIN
******************************************/

var addTopicTests = []struct {
	Topic       TopicData
	ErrExpected bool
}{
	{TopicData{Name: "test1"}, false},
	{TopicData{Name: ""}, true},
	{TopicData{
		Name:   "test3",
		Schema: dataSchema,
	}, false},
	{TopicData{Name: "test1"}, true},
}

func checkAddTopicQuery(query string, topic TopicData, id string) bool {
	schemaStr := "{}"
	if topic.Schema != nil {
		schemaBytes, err := json.Marshal(topic.Schema)
		if err != nil {
			return false
		}
		schemaStr = string(schemaBytes)
	}

	schemaStr = strings.Replace(schemaStr, "[", "\\[", -1)
	schemaStr = strings.Replace(schemaStr, "]", "\\]", -1)

	exp := fmt.Sprintf(`^INSERT INTO event_topic \(topic_id, topic_name, data_schema\)[\s\S]*VALUES \(%s, %s, %s\);$`,
		id, stringify(topic.Name), stringify(schemaStr))
	return regexp.MustCompile(exp).MatchString(query)
}

func TestAddTopic(t *testing.T) {
	testESServer := NewMockESServer()
	defer testESServer.Close()

	s, err := GetTestEventStore(testESServer)
	assert.Nil(t, err)

	for _, test := range addTopicTests {
		id, err := s.AddTopic(test.Topic)
		assert.Equal(t, test.ErrExpected, err != nil)
		if !test.ErrExpected {
			assert.True(t, isUUID(id))
			assert.True(t, checkAddTopicQuery(s.cqlSession.(*cass.MockCassSession).LastQuery(), test.Topic, id))
		}
	}
}

var deleteTopicTests = []struct {
	DeleteReq   *eventmaster.DeleteTopicRequest
	ErrExpected bool
	NumTopics   int
}{
	{&eventmaster.DeleteTopicRequest{TopicName: "test1"}, false, 2},
	{&eventmaster.DeleteTopicRequest{TopicName: "test1"}, true, 2},
	{&eventmaster.DeleteTopicRequest{TopicName: "test3"}, false, 1},
	{&eventmaster.DeleteTopicRequest{TopicName: "nonexistent"}, true, 1},
}

func checkDeleteTopicQuery(query string, id string) bool {
	exp := fmt.Sprintf(`DELETE FROM event_topic WHERE topic_id=%s;`, id)
	return query == exp
}

func TestDeleteTopic(t *testing.T) {
	testESServer := NewMockESServer()
	defer testESServer.Close()

	s, err := GetTestEventStore(testESServer)
	assert.Nil(t, err)

	err = populateTopics(s)
	assert.Nil(t, err)

	for _, test := range deleteTopicTests {
		id := s.topicNameToId[test.DeleteReq.TopicName]

		err := s.DeleteTopic(test.DeleteReq)
		assert.Equal(t, test.ErrExpected, err != nil)
		assert.Equal(t, test.NumTopics, len(s.topicNameToId))

		if !test.ErrExpected {
			assert.True(t, checkDeleteTopicQuery(s.cqlSession.(*cass.MockCassSession).LastQuery(), id))
		}
	}
}

var updateTopicTests = []struct {
	Name          string
	Topic         TopicData
	ErrExpected   bool
	ExpectedQuery string
}{
	{"test1", TopicData{Name: "test4"}, false,
		"UPDATE event_topic SET topic_name='test4' WHERE topic_id=%s;"},
	{"test1", TopicData{Name: "test2"}, true, ""},
	{"test2", TopicData{Name: "test4"}, true, ""},
	{"test3", TopicData{Schema: map[string]interface{}{
		"title":       "test",
		"description": "test",
		"type":        "object",
		"required":    []interface{}{"last_name"},
		"properties": map[string]interface{}{
			"first_name": map[string]interface{}{
				"type":    "string",
				"default": "bob",
			},
			"last_name": map[string]interface{}{
				"type": "string",
			},
			"user_id": map[string]interface{}{
				"type":    "integer",
				"minimum": 0,
			},
		},
	}}, true, ""},
	{"test3", TopicData{Schema: map[string]interface{}{
		"title":       "test",
		"description": "test",
		"type":        "object",
		"required":    []interface{}{"user_id", "first_name"},
		"properties": map[string]interface{}{
			"first_name": map[string]interface{}{
				"type":    "string",
				"default": "bob",
			},
			"last_name": map[string]interface{}{
				"type": "string",
			},
			"user_id": map[string]interface{}{
				"type":    "integer",
				"minimum": 0,
			},
		},
	}}, false, ""},
}

func TestUpdateTopic(t *testing.T) {
	testESServer := NewMockESServer()
	defer testESServer.Close()

	s, err := GetTestEventStore(testESServer)
	assert.Nil(t, err)

	err = populateTopics(s)
	assert.Nil(t, err)

	for _, test := range updateTopicTests {
		id, err := s.UpdateTopic(test.Name, test.Topic)
		assert.Equal(t, test.ErrExpected, err != nil)

		if test.ExpectedQuery != "" {
			assert.True(t, isUUID(id))
			assert.Equal(t, fmt.Sprintf(test.ExpectedQuery, id), s.cqlSession.(*cass.MockCassSession).LastQuery())
		}
	}
}

/******************************************
	DC TESTS BEGIN
******************************************/

var addDcTests = []struct {
	Dc          *eventmaster.Dc
	ErrExpected bool
}{
	{&eventmaster.Dc{DcName: "dc1"}, false},
	{&eventmaster.Dc{DcName: "dc2"}, false},
	{&eventmaster.Dc{DcName: "dc1"}, true},
}

func TestAddDc(t *testing.T) {
	testESServer := NewMockESServer()
	defer testESServer.Close()

	s, err := GetTestEventStore(testESServer)
	assert.Nil(t, err)

	for _, test := range addDcTests {
		id, err := s.AddDc(test.Dc)
		assert.Equal(t, test.ErrExpected, err != nil)

		if !test.ErrExpected {
			assert.True(t, isUUID(id))
			expectedQ := fmt.Sprintf("INSERT INTO event_dc (dc_id, dc) VALUES (%s, %s);",
				id, stringify(test.Dc.DcName))
			assert.Equal(t, expectedQ, s.cqlSession.(*cass.MockCassSession).LastQuery())
		}
	}
}

var updateDcTests = []struct {
	Req         *eventmaster.UpdateDcRequest
	ErrExpected bool
}{
	{&eventmaster.UpdateDcRequest{OldName: "dc2", NewName: "dc4"}, false},
	{&eventmaster.UpdateDcRequest{OldName: "dc1", NewName: ""}, true},
	{&eventmaster.UpdateDcRequest{OldName: "dc9", NewName: "hello"}, true},
	{&eventmaster.UpdateDcRequest{OldName: "dc1", NewName: "dc3"}, true},
}

func TestUpdateDc(t *testing.T) {
	testESServer := NewMockESServer()
	defer testESServer.Close()

	s, err := GetTestEventStore(testESServer)
	assert.Nil(t, err)

	err = populateDcs(s)
	assert.Nil(t, err)

	for _, test := range updateDcTests {
		id, err := s.UpdateDc(test.Req)
		assert.Equal(t, test.ErrExpected, err != nil)

		if !test.ErrExpected {
			assert.True(t, isUUID(id))
			expectedQ := fmt.Sprintf("UPDATE event_dc SET dc=%s WHERE dc_id=%s;",
				stringify(test.Req.NewName), id)
			assert.Equal(t, expectedQ, s.cqlSession.(*cass.MockCassSession).LastQuery())
		}
	}
}

/******************************************
	EVENT TESTS BEGIN
******************************************/

var addEventTests = []struct {
	Event       *UnaddedEvent
	ErrExpected bool
}{
	{&UnaddedEvent{
		ParentEventID: "4e54cfd7-e49c-41ea-820b-62c1b241a80a",
		Dc:            "dc1",
		TopicName:     "test1",
		Tags:          []string{"tag1", "tag2"},
		Host:          "host1",
		TargetHosts:   []string{"thost1", "thost2"},
		User:          "user",
		Data: map[string]interface{}{
			"ses": 3,
			"oid": "123",
		},
	}, false},
	{&UnaddedEvent{
		Dc:        "dc2",
		TopicName: "test2",
		Tags:      []string{"tag1", "tag2"},
		Host:      "host1",
		User:      "user",
	}, false},
	{&UnaddedEvent{
		Dc:        "dc2",
		TopicName: "test3",
		Tags:      []string{"tag1", "tag2"},
		User:      "user",
	}, true},
	{&UnaddedEvent{
		TopicName: "test3",
		Tags:      []string{"tag1", "tag2"},
		Host:      "host1",
		User:      "user",
	}, true},
	{&UnaddedEvent{
		Dc:        "nope",
		TopicName: "test3",
		Tags:      []string{"tag1", "tag2"},
		Host:      "host1",
		User:      "user",
	}, true},
	{&UnaddedEvent{
		Dc:        "dc2",
		TopicName: "nope",
		Tags:      []string{"tag1", "tag2"},
		Host:      "host1",
		User:      "user",
	}, true},
	{&UnaddedEvent{
		Dc:   "dc2",
		Tags: []string{"tag1", "tag2"},
		Host: "host1",
		User: "user",
	}, true},
	{&UnaddedEvent{
		Dc:        "dc2",
		TopicName: "test3",
		Tags:      []string{"tag1", "tag2"},
		Host:      "host1",
		Data: map[string]interface{}{
			"user_id":     234,
			"middle_name": "alice",
		},
	}, false},
	{&UnaddedEvent{
		Dc:        "dc2",
		TopicName: "test3",
		Tags:      []string{"tag1", "tag2"},
		Host:      "host1",
		Data: map[string]interface{}{
			"user_id": "wrong_type",
		},
	}, true},
	{&UnaddedEvent{
		Dc:        "dc2",
		TopicName: "test3",
		Tags:      []string{"tag1", "tag2"},
		Host:      "host1",
	}, true},
}

func checkAddEventQuery(query string, id string, evt *UnaddedEvent) bool {
	data, err := json.Marshal(evt.Data)
	if err != nil {
		return false
	}

	matchStr := fmt.Sprintf(`[\s\S]*BEGIN BATCH[\s\S]*INSERT INTO event \(event_id, parent_event_id, dc_id, topic_id, host, target_host_set, user, event_time, tag_set, data_json, received_time\)[\s\S]*VALUES \(%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]s, %[7]s, %[8]s, %[9]s, %[10]s, %[11]s\);[\s\S]*INSERT INTO temp_event \(event_id, parent_event_id, dc_id, topic_id, host, target_host_set, user, event_time, tag_set, data_json, received_time\)[\s\S]*VALUES \(%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]s, %[7]s, %[8]s, %[9]s, %[10]s, %[11]s\);[\s\S]*APPLY BATCH;`,
		id, stringifyUUID(evt.ParentEventID), uuidMatchStr, uuidMatchStr,
		stringify(evt.Host), stringifyArr(evt.TargetHosts), stringify(evt.User), "\\d{10}000",
		stringifyArr(evt.Tags), stringify(string(data)), "\\d{10}000")

	return regexp.MustCompile(matchStr).MatchString(query)
}

func TestAddEvent(t *testing.T) {
	testESServer := NewMockESServer()
	defer testESServer.Close()

	s, err := GetTestEventStore(testESServer)
	assert.Nil(t, err)

	err = populateTopics(s)
	assert.Nil(t, err)

	err = populateDcs(s)
	assert.Nil(t, err)

	for _, test := range addEventTests {
		id, err := s.AddEvent(test.Event)
		assert.Equal(t, test.ErrExpected, err != nil)
		if !test.ErrExpected {
			assert.True(t, isUUID(id))
			assert.True(t, checkAddEventQuery(s.cqlSession.(*cass.MockCassSession).LastQuery(), id, test.Event))
		}
	}
}

var buildESQueryTests = []struct {
	Query           *eventmaster.Query
	ExpectedSource  string
	NeedsFormatting bool
}{
	{&eventmaster.Query{}, "{\"bool\":{}}", false},
	{&eventmaster.Query{
		Dc:             []string{"dc1", "dc2"},
		Host:           []string{"hostname"},
		TopicName:      []string{"test1"},
		User:           []string{"user1", "user2"},
		StartEventTime: 1500328222,
	}, "{\"bool\":{\"must\":[{\"query_string\":{\"query\":\"dc_id:(%s OR %s)\"}},{\"query_string\":{\"query\":\"host:(hostname)\"}},{\"query_string\":{\"query\":\"topic_id:(%s)\"}},{\"query_string\":{\"query\":\"user:(user1 OR user2)\"}},{\"range\":{\"event_time\":{\"from\":1500328222000,\"include_lower\":true,\"include_upper\":true,\"to\":null}}}]}}", true},
	{&eventmaster.Query{
		TargetHostSet: []string{"host1", "host2"},
		ParentEventId: []string{"d6f377e0-eeed-4cdc-8ba3-ae47018bb80d", "a0d18d31-a5e5-436f-be64-9990e3fc4850"},
		TagSet:        []string{"tag1", "tag2"},
		User:          []string{"user2"},
		EndEventTime:  1500328222,
	}, "{\"bool\":{\"must\":[{\"terms\":{\"target_host_set\":[\"host1\",\"host2\"]}},{\"terms\":{\"tag_set\":[\"tag1\",\"tag2\"]}},{\"query_string\":{\"query\":\"parent_event_id:(d6f377e0-eeed-4cdc-8ba3-ae47018bb80d OR a0d18d31-a5e5-436f-be64-9990e3fc4850)\"}},{\"query_string\":{\"query\":\"user:(user2)\"}},{\"range\":{\"event_time\":{\"from\":null,\"include_lower\":true,\"include_upper\":true,\"to\":1500328222000}}}]}}", false},
	{&eventmaster.Query{
		Dc:        []string{"dc100"},
		Host:      []string{"host1"},
		TopicName: []string{"test100"},
	}, "{\"bool\":{\"must\":[{\"query_string\":{\"query\":\"dc_id:()\"}},{\"query_string\":{\"query\":\"host:(host1)\"}},{\"query_string\":{\"query\":\"topic_id:()\"}}]}}", false},
	{&eventmaster.Query{
		Dc:             []string{"dc1"},
		Data:           "fulltextsearch",
		StartEventTime: 1500328222,
		EndEventTime:   1500329000,
	}, "{\"bool\":{\"must\":[{\"query_string\":{\"query\":\"dc_id:(%s)\"}},{\"query_string\":{\"query\":\"fulltextsearch\"}},{\"range\":{\"event_time\":{\"from\":1500328222000,\"include_lower\":true,\"include_upper\":true,\"to\":1500329000000}}}]}}", true},
	{&eventmaster.Query{
		Data: "{\"key1\": {\"value1\": \"morevalue\"}, \"key2\": \"value2\"}",
	}, "{\"bool\":{\"must\":[{\"term\":{\"data.key1.value1\":\"morevalue\"}},{\"term\":{\"data.key2\":\"value2\"}}]}}", false},
}

func TestBuildESQuery(t *testing.T) {
	testESServer := NewMockESServer()
	defer testESServer.Close()

	s, err := GetTestEventStore(testESServer)
	assert.Nil(t, err)

	err = populateTopics(s)
	assert.Nil(t, err)

	err = populateDcs(s)
	assert.Nil(t, err)

	for _, test := range buildESQueryTests {
		bq := s.buildESQuery(test.Query)
		source, err := bq.Source()
		assert.Nil(t, err)
		b, err := json.Marshal(source)
		assert.Nil(t, err)

		expected := strings.Replace(test.ExpectedSource, "(", "\\(", -1)
		expected = strings.Replace(expected, ")", "\\)", -1)
		expected = strings.Replace(expected, "[", "\\[", -1)
		expected = strings.Replace(expected, "]", "\\]", -1)
		expected = strings.Replace(expected, "%s", "%[1]s", -1)

		matchStr := expected
		actual := string(b)
		if test.NeedsFormatting {
			matchStr = fmt.Sprintf(expected, uuidMatchStr)
		}

		r := regexp.MustCompile(matchStr)
		assert.True(t, r.MatchString(actual))
	}
}
