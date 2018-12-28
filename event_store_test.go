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

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/assert"
	"github.com/xeipuuv/gojsonschema"

	"github.com/wish/eventmaster/cassandra"
	eventmaster "github.com/wish/eventmaster/proto"
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

var testDCs = []*eventmaster.DC{
	&eventmaster.DC{DCName: "dc1"},
	&eventmaster.DC{DCName: "dc2"},
	&eventmaster.DC{DCName: "dc3"},
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

func populateDCs(s *EventStore) error {
	for _, dc := range testDCs {
		_, err := s.AddDC(dc)
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

func NewNoOpDataStore() DataStore {
	return &CassandraStore{
		session: &cassandra.MockCassSession{},
	}
}

func GetTestEventStore(ds DataStore) (*EventStore, error) {
	ev := &EventStore{
		ds:                       ds,
		topicMutex:               &sync.RWMutex{},
		dcMutex:                  &sync.RWMutex{},
		indexMutex:               &sync.RWMutex{},
		topicNameToID:            make(map[string]string),
		topicIDToName:            make(map[string]string),
		topicSchemaMap:           make(map[string]*gojsonschema.Schema),
		topicSchemaPropertiesMap: make(map[string](map[string]interface{})),
		dcNameToID:               make(map[string]string),
		dcIDToName:               make(map[string]string),
	}
	return ev, nil
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

	s, err := GetTestEventStore(NewNoOpDataStore())
	assert.Nil(t, err)

	for _, test := range addTopicTests {
		id, err := s.AddTopic(test.Topic)
		assert.Equal(t, test.ErrExpected, err != nil)
		if !test.ErrExpected {
			assert.True(t, isUUID(id))
			assert.True(t, checkAddTopicQuery(s.ds.(*CassandraStore).session.(*cassandra.MockCassSession).LastQuery(), test.Topic, id))
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

	s, err := GetTestEventStore(NewNoOpDataStore())
	assert.Nil(t, err)

	err = populateTopics(s)
	assert.Nil(t, err)

	for _, test := range deleteTopicTests {
		id := s.topicNameToID[test.DeleteReq.TopicName]

		err := s.DeleteTopic(test.DeleteReq)
		assert.Equal(t, test.ErrExpected, err != nil)
		assert.Equal(t, test.NumTopics, len(s.topicNameToID))

		if !test.ErrExpected {
			assert.True(t, checkDeleteTopicQuery(s.ds.(*CassandraStore).session.(*cassandra.MockCassSession).LastQuery(), id))
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

	s, err := GetTestEventStore(NewNoOpDataStore())
	assert.Nil(t, err)

	err = populateTopics(s)
	assert.Nil(t, err)

	for _, test := range updateTopicTests {
		id, err := s.UpdateTopic(test.Name, test.Topic)
		assert.Equal(t, test.ErrExpected, err != nil)

		if test.ExpectedQuery != "" {
			assert.True(t, isUUID(id))
			assert.True(t, regexp.MustCompile(fmt.Sprintf(test.ExpectedQuery, id)).MatchString(s.ds.(*CassandraStore).session.(*cassandra.MockCassSession).LastQuery()))
		}
	}
}

/******************************************
	DC TESTS BEGIN
******************************************/

var addDCTests = []struct {
	DC          *eventmaster.DC
	ErrExpected bool
}{
	{&eventmaster.DC{DCName: "dc1"}, false},
	{&eventmaster.DC{DCName: "dc2"}, false},
	{&eventmaster.DC{DCName: "dc1"}, true},
}

func TestAddDC(t *testing.T) {
	testESServer := NewMockESServer()
	defer testESServer.Close()

	s, err := GetTestEventStore(NewNoOpDataStore())
	assert.Nil(t, err)

	for _, test := range addDCTests {
		id, err := s.AddDC(test.DC)
		assert.Equal(t, test.ErrExpected, err != nil)

		if !test.ErrExpected {
			assert.True(t, isUUID(id))
			exp := fmt.Sprintf(`^INSERT INTO event_dc[\s\S]*\(dc_id, dc\)[\s\S]*VALUES \(%s, %s\);$`,
				id, stringify(test.DC.DCName))
			assert.True(t, regexp.MustCompile(exp).MatchString(s.ds.(*CassandraStore).session.(*cassandra.MockCassSession).LastQuery()))
		}
	}
}

var updateDCTests = []struct {
	Req         *eventmaster.UpdateDCRequest
	ErrExpected bool
}{
	{&eventmaster.UpdateDCRequest{OldName: "dc2", NewName: "dc4"}, false},
	{&eventmaster.UpdateDCRequest{OldName: "dc1", NewName: ""}, true},
	{&eventmaster.UpdateDCRequest{OldName: "dc9", NewName: "hello"}, true},
	{&eventmaster.UpdateDCRequest{OldName: "dc1", NewName: "dc3"}, true},
}

func TestUpdateDC(t *testing.T) {
	testESServer := NewMockESServer()
	defer testESServer.Close()

	s, err := GetTestEventStore(NewNoOpDataStore())
	assert.Nil(t, err)

	err = populateDCs(s)
	assert.Nil(t, err)

	for _, test := range updateDCTests {
		id, err := s.UpdateDC(test.Req)
		assert.Equal(t, test.ErrExpected, err != nil)

		if !test.ErrExpected {
			assert.True(t, isUUID(id))
			expectedQ := fmt.Sprintf("UPDATE event_dc SET dc=%s WHERE dc_id=%s;",
				stringify(test.Req.NewName), id)
			assert.Equal(t, expectedQ, s.ds.(*CassandraStore).session.(*cassandra.MockCassSession).LastQuery())
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
		DC:            "dc1",
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
		DC:        "dc2",
		TopicName: "test2",
		Tags:      []string{"tag1", "tag2"},
		Host:      "host1",
		User:      "user",
	}, false},
	{&UnaddedEvent{
		DC:        "dc2",
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
		DC:        "nope",
		TopicName: "test3",
		Tags:      []string{"tag1", "tag2"},
		Host:      "host1",
		User:      "user",
	}, true},
	{&UnaddedEvent{
		DC:        "dc2",
		TopicName: "nope",
		Tags:      []string{"tag1", "tag2"},
		Host:      "host1",
		User:      "user",
	}, true},
	{&UnaddedEvent{
		DC:   "dc2",
		Tags: []string{"tag1", "tag2"},
		Host: "host1",
		User: "user",
	}, true},
	{&UnaddedEvent{
		DC:        "dc2",
		TopicName: "test3",
		Tags:      []string{"tag1", "tag2"},
		Host:      "host1",
		Data: map[string]interface{}{
			"user_id":     234,
			"middle_name": "alice",
		},
	}, false},
	{&UnaddedEvent{
		DC:        "dc2",
		TopicName: "test3",
		Tags:      []string{"tag1", "tag2"},
		Host:      "host1",
		Data: map[string]interface{}{
			"user_id": "wrong_type",
		},
	}, true},
	{&UnaddedEvent{
		DC:        "dc2",
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

	s, err := GetTestEventStore(NewNoOpDataStore())
	assert.Nil(t, err)

	err = populateTopics(s)
	assert.Nil(t, err)

	err = populateDCs(s)
	assert.Nil(t, err)

	for _, test := range addEventTests {
		id, err := s.AddEvent(test.Event)
		assert.Equal(t, test.ErrExpected, err != nil)
		if !test.ErrExpected {
			_, err := ksuid.Parse(id)
			assert.Nil(t, err)
			assert.True(t, checkAddEventQuery(s.ds.(*CassandraStore).session.(*cassandra.MockCassSession).LastQuery(), id, test.Event))
		}
	}
}

func PopulateTestData(es *EventStore) error {
	for i := 0; i < 5; i++ {
		dc := &eventmaster.DC{
			ID:     uuid.NewV4().String(),
			DCName: fmt.Sprintf("dc%04d", i),
		}
		if _, err := es.AddDC(dc); err != nil {
			return errors.Wrapf(err, "adding dc: %v", dc)
		}
		t := Topic{
			ID:   uuid.NewV4().String(),
			Name: fmt.Sprintf("t%04d", i),
		}
		if _, err := es.AddTopic(t); err != nil {
			return errors.Wrapf(err, "adding topic: %v", t)
		}
	}
	return nil
}
