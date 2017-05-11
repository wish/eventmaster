package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/ContextLogic/eventmaster/eventmaster"
	"github.com/gocql/gocql"
	"github.com/satori/go.uuid"
)

type EventStore struct {
	session *gocql.Session
}

func NewEventStore(s *gocql.Session) *EventStore {
	return &EventStore{
		session: s,
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

func generateInsertQuery(event *eventmaster.Event, date string, logID string) string {
	return fmt.Sprintf(`BEGIN BATCH
    INSERT INTO event_by_topic (date, dc, topic_name, event_time, log_id, tags) 
    VALUES (%[1]s, %[2]s, %[3]s, %[6]d, %[7]s, %[8]s);
    INSERT INTO event_by_host (date, dc, host_name, user, event_time, log_id)
    VALUES (%[1]s, %[2]s, %[4]s, %[5]s, %[6]d, %[7]s);
    INSERT INTO event_by_day (date, dc, event_time, log_id)
    VALUES (%[1]s, %[2]s, %[5]s, %[7]s);
    INSERT INTO event_logs (date, dc, topic_name, host, user, event_time, log_id, tags, data)
    VALUES (%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]d, %[7]s, %[8]s, %[9]s);
    APPLY BATCH;`,
		stringify(date), stringify(event.Dc), stringify(event.TopicName), stringify(event.Host),
		stringify(event.User), event.Timestamp, logID,
		stringifyArr(event.Tags), stringify(event.Data))
}

func (es *EventStore) AddEvent(event *eventmaster.Event) error {
	date := time.Unix(event.Timestamp, 0).UTC().Format("2006-01-02")
	logID := uuid.NewV4().String()

	queryStr := generateInsertQuery(event, date, logID)
	query := es.session.Query(queryStr)

	if err := query.Exec(); err != nil {
		return err
	}
	return nil
}
