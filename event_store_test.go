package eventmaster

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	eventmaster "github.com/ContextLogic/eventmaster/proto"
	cass "github.com/ContextLogic/eventmaster/src/cassandra_client"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/assert"
	"github.com/xeipuuv/gojsonschema"
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

var testTopics = []Topic{
	Topic{Name: "test1"},
	Topic{Name: "test2"},
	Topic{Name: "test3", Schema: dataSchema},
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

func NewMockDataStore() DataStore {
	return &CassandraStore{
		session: &cass.MockCassSession{},
	}
}

func GetTestEventStore(testESServer *httptest.Server) (*EventStore, error) {
	ds := NewMockDataStore()
	return &EventStore{
		ds:                       ds,
		topicMutex:               &sync.RWMutex{},
		dcMutex:                  &sync.RWMutex{},
		indexMutex:               &sync.RWMutex{},
		topicNameToId:            make(map[string]string),
		topicIdToName:            make(map[string]string),
		topicSchemaMap:           make(map[string]*gojsonschema.Schema),
		topicSchemaPropertiesMap: make(map[string](map[string]interface{})),
		dcNameToId:               make(map[string]string),
		dcIdToName:               make(map[string]string),
	}, nil
}

/******************************************
	TOPIC TESTS BEGIN
******************************************/

var addTopicTests = []struct {
	Topic       Topic
	ErrExpected bool
}{
	{Topic{Name: "test1"}, false},
	{Topic{Name: ""}, true},
	{Topic{
		Name:   "test3",
		Schema: dataSchema,
	}, false},
	{Topic{Name: "test1"}, true},
}

func checkAddTopicQuery(query string, topic Topic, id string) bool {
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

	exp := fmt.Sprintf(`^INSERT INTO event_topic[\s\S]*\(topic_id, topic_name, data_schema\)[\s\S]*VALUES \(%s, %s, %s\);$`,
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
			assert.True(t, checkAddTopicQuery(s.ds.(*CassandraStore).session.(*cass.MockCassSession).LastQuery(), test.Topic, id))
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
			assert.True(t, checkDeleteTopicQuery(s.ds.(*CassandraStore).session.(*cass.MockCassSession).LastQuery(), id))
		}
	}
}

var updateTopicTests = []struct {
	Name          string
	Topic         Topic
	ErrExpected   bool
	ExpectedQuery string
}{
	{"test1", Topic{Name: "test4"}, false,
		`^UPDATE event_topic SET[\s\S]*topic_name='test4',[\s\S]*data_schema='{}'[\s\S]*WHERE topic_id=%s;$`},
	{"test1", Topic{Name: "test2"}, true, ""},
	{"test2", Topic{Name: "test4"}, true, ""},
	{"test3", Topic{Schema: map[string]interface{}{
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
	{"test3", Topic{Schema: map[string]interface{}{
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
			assert.True(t, regexp.MustCompile(fmt.Sprintf(test.ExpectedQuery, id)).MatchString(s.ds.(*CassandraStore).session.(*cass.MockCassSession).LastQuery()))
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
			exp := fmt.Sprintf(`^INSERT INTO event_dc[\s\S]*\(dc_id, dc\)[\s\S]*VALUES \(%s, %s\);$`,
				id, stringify(test.Dc.DcName))
			assert.True(t, regexp.MustCompile(exp).MatchString(s.ds.(*CassandraStore).session.(*cass.MockCassSession).LastQuery()))
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
			assert.Equal(t, expectedQ, s.ds.(*CassandraStore).session.(*cass.MockCassSession).LastQuery())
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
		ParentEventID: "0ra0GxvIDmaFkVr0pqx6EVYcuzD",
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

	unixTimestamp := time.Now().Unix()
	if evt.EventTime != 0 {
		unixTimestamp = evt.EventTime
	}
	date := getDate(unixTimestamp)

	matchStr := fmt.Sprintf(`[\s\S]*BEGIN BATCH[\s\S]*INSERT INTO event \(event_id, parent_event_id, dc_id, topic_id, host, target_host_set, user, event_time, tag_set, received_time, date\)[\s\S]*VALUES \(%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]s, %[7]s, %[8]s, %[9]s, %[11]s, %[12]s\);[\s\S]*INSERT INTO event_metadata\(event_id, data_json\)[\s\S]*VALUES \(%[1]s, \$\$%[10]s\$\$\);[\s\S]*INSERT INTO event_by_topic\(event_id, topic_id, event_time, date\)[\s\S]*VALUES \(%[1]s, %[4]s, %[8]s, %[12]s\);[\s\S]*INSERT INTO event_by_dc\(event_id, dc_id, event_time, date\)[\s\S]*VALUES \(%[1]s, %[3]s, %[8]s, %[12]s\);[\s\S]*INSERT INTO event_by_host\(event_id, host, event_time, date\)[\s\S]*VALUES \(%[1]s, %[5]s, %[8]s, %[12]s\);[\s\S]*INSERT INTO event_by_date\(event_id, event_time, date\)[\s\S]*VALUES \(%[1]s, %[8]s, %[12]s\);[\s\S]*APPLY BATCH;`,
		stringify(id), stringify(evt.ParentEventID), uuidMatchStr, uuidMatchStr,
		stringify(evt.Host), stringifyArr(evt.TargetHosts), stringify(evt.User), "\\d{10}000",
		stringifyArr(evt.Tags), string(data), "\\d{10}000", stringify(date))

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
			_, err := ksuid.Parse(id)
			assert.Nil(t, err)
			assert.True(t, checkAddEventQuery(s.ds.(*CassandraStore).session.(*cass.MockCassSession).LastQuery(), id, test.Event))
		}
	}
}
