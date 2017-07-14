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

func isUUID(uuid string) bool {
	r := regexp.MustCompile("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-4[a-fA-F0-9]{3}-[8|9|aA|bB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}$")
	return r.MatchString(uuid)
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

var testTopics = []TopicData{
	TopicData{Name: "test1"},
	TopicData{Name: "test2"},
	TopicData{Name: "test3", Schema: dataSchema},
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
			assert.Equal(t, fmt.Sprintf(test.ExpectedQuery, id), s.cqlSession.(*cass.MockCassSession).LastQuery())
		}
	}
}
