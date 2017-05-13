package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ContextLogic/eventmaster/eventmaster"
	"github.com/gocql/gocql"
	"github.com/satori/go.uuid"
)

type StringFilter uint8

const (
	EQUALS_S StringFilter = iota
	CONTAINS
	STARTS_WITH
	ENDS_WITH
)

type TimeFilter uint8

const (
	BEFORE StringFilter = iota
	AFTER
	BETWEEN
	EQUALS_T
)

type Query struct {
	Dc           string
	Host         string
	TopicName    string
	User         string
	Date         string
	Time         int64
	TimeEnd      int64
	stringFilter StringFilter
	timeFilter   TimeFilter
}

type FullEvent struct {
	Timestamp int64
	Dc        string
	TopicName string
	Tags      []string
	Host      string
	User      string
	Data      string
	Date      string
	LogID     string
}

func augmentEvent(event *eventmaster.Event) *FullEvent {
	date := time.Unix(event.Timestamp, 0).UTC().Format("2006-01-02")
	logID := uuid.NewV4().String()
	return &FullEvent{
		Timestamp: event.Timestamp,
		Dc:        event.Dc,
		TopicName: event.TopicName,
		Tags:      event.Tags,
		Host:      event.Host,
		User:      event.User,
		Data:      event.Data,
		Date:      date,
		LogID:     logID,
	}
}

func stringify(str string) string {
	return fmt.Sprintf("'%s'", str)
}

func stringifyArr(arr []string) string {
	for i, str := range arr {
		arr[i] = stringify(str)
	}
	return fmt.Sprintf("[%s]", strings.Join(arr, ","))
}

func generateInsertQuery(event *FullEvent) string {
	return fmt.Sprintf(`
	BEGIN BATCH
    INSERT INTO event_logs (date, dc, topic_name, host, user, event_time, log_id, tags, data)
    VALUES (%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]d, %[7]s, %[8]s, %[9]s);
    INSERT INTO event_topics (topic_name)
    VALUES (%[3]s) IF NOT EXISTS;
    APPLY BATCH`,
		stringify(event.Date), stringify(event.Dc), stringify(event.TopicName), stringify(event.Host),
		stringify(event.User), event.Timestamp, event.LogID,
		stringifyArr(event.Tags), stringify(event.Data))
}

type EventStore struct {
	session *gocql.Session
	dc      string
}

func NewEventStore(s *gocql.Session) *EventStore {
	return &EventStore{
		session: s,
	}
}

func (es *EventStore) AddEvent(event *eventmaster.Event) error {
	fe := augmentEvent(event)
	queryStr := generateInsertQuery(fe)
	query := es.session.Query(queryStr)

	if err := query.Exec(); err != nil {
		return err
	}
	fmt.Println("Event added:", fe.LogID)
	return nil
}

func (es *EventStore) FindNextResults(iter *gocql.Iter) ([]*FullEvent, error) {
	var events []*FullEvent
	var id gocql.UUID
	for i := 1; i <= 50; i++ {
		if ok := iter.Scan(&id); !ok {
			return events, nil
		}
		results, err := es.session.Query(fmt.Sprintf(`SELECT *
            FROM event_logs
            WHERE log_id = %s AND dc = '%s'`, id, es.dc)).Iter().SliceMap()
		if err != nil {
			return nil, err
		}

		if len(results) < 1 {
			return nil, errors.New("Could not find log with log id: " + id.String())
		}
		result := results[0]

		fe := &FullEvent{
			Timestamp: result["event_time"].(time.Time).Unix(),
			Data:      result["data"].(string),
			LogID:     result["log_id"].(gocql.UUID).String(),
			TopicName: result["topic_name"].(string),
			Host:      result["host"].(string),
			User:      result["user"].(string),
			Date:      result["date"].(string),
			Dc:        result["dc"].(string),
			Tags:      result["tags"].([]string),
		}
		events = append(events, fe)
	}
	return events, nil
}

func (es *EventStore) FindByTopic(topic string, dc string, page int) ([]*FullEvent, error) {
	es.dc = dc
	iter := es.session.Query(fmt.Sprintf(`SELECT log_id 
        FROM event_by_topic 
        WHERE topic_name = '%s' AND dc = '%s'`, topic, dc)).Iter()

	return es.FindNextResults(iter)
}

func (es *EventStore) FindByDate(date string, dc string, page int) ([]*FullEvent, error) {
	es.dc = dc
	iter := es.session.Query(fmt.Sprintf(`SELECT log_id 
        FROM event_by_day 
        WHERE date = '%s' AND dc = '%s'`, date, dc)).Iter()

	return es.FindNextResults(iter)
}

func (es *EventStore) FindByHost(host string, dc string, page int) ([]*FullEvent, error) {
	es.dc = dc
	iter := es.session.Query(fmt.Sprintf(`SELECT log_id 
        FROM event_by_host 
        WHERE host = '%s' AND dc = '%s'
        ORDER BY event_time DESC`, host, dc)).Iter()

	return es.FindNextResults(iter)
}

func (es *EventStore) CloseSession() {
	es.session.Close()
}
