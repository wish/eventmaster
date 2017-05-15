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

type Query struct {
	Dc        string
	Host      string
	TopicName string
	TimeStart int64
	TimeEnd   int64
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

func buildInsertQuery(event *FullEvent) string {
	return fmt.Sprintf(`
	BEGIN BATCH
    INSERT INTO event_logs (date, dc, topic_name, host, user, event_time, log_id, tags, data)
    VALUES (%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]d, %[7]s, %[8]s, %[9]s);
    INSERT INTO event_topics (topic_name)
    VALUES (%[3]s);
    INSERT INTO event_dcs (dc)
    VALUES (%[2]s)
    APPLY BATCH`,
		stringify(event.Date), stringify(event.Dc), stringify(event.TopicName), stringify(event.Host),
		stringify(event.User), event.Timestamp, event.LogID,
		stringifyArr(event.Tags), stringify(event.Data))
}

func buildSelectQuery(q *Query) string {
	firstFilter := true
	s := []string{"SELECT log_id FROM indexed_events"}
	if q.Dc != "" {
		if firstFilter {
			firstFilter = false
			s = append(s, fmt.Sprintf(` WHERE dc=%s`, stringify(q.Dc)))
		} else {
			s = append(s, fmt.Sprintf(` AND dc=%s`, stringify(q.Dc)))
		}
	}
	if q.Host != "" {
		if firstFilter {
			firstFilter = false
			s = append(s, fmt.Sprintf(` WHERE host=%s`, stringify(q.Host)))
		} else {
			s = append(s, fmt.Sprintf(` AND host=%s`, stringify(q.Host)))
		}
	}
	if q.TopicName != "" {
		if firstFilter {
			firstFilter = false
			s = append(s, fmt.Sprintf(` WHERE topic_name=%s`, stringify(q.TopicName)))
		} else {
			s = append(s, fmt.Sprintf(` AND topic_name=%s`, stringify(q.TopicName)))
		}
	}
	if q.TimeStart != -1 {
		if firstFilter {
			firstFilter = false
			s = append(s, fmt.Sprintf(` WHERE timestamp>=%d`, q.TimeStart))
		} else {
			s = append(s, fmt.Sprintf(` AND timestamp>=%d`, q.TimeStart))
		}
	}
	if q.TimeEnd != -1 {
		if firstFilter {
			firstFilter = false
			s = append(s, fmt.Sprintf(` WHERE timestamp<=%d`, q.TimeEnd))
		} else {
			s = append(s, fmt.Sprintf(` AND timestamp<=%d`, q.TimeEnd))
		}
	}
	s = append(s, " ALLOW FILTERING;")
	return strings.Join(s, "")
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

func (es *EventStore) getFullEvents(iter *gocql.Iter) ([]*FullEvent, error) {
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

func (es *EventStore) AddEvent(event *eventmaster.Event) error {
	fe := augmentEvent(event)
	queryStr := buildInsertQuery(fe)
	query := es.session.Query(queryStr)

	if err := query.Exec(); err != nil {
		return err
	}
	fmt.Println("Event added:", fe.LogID)
	return nil
}

func (es *EventStore) Find(q *Query) ([]*FullEvent, error) {
	query := buildSelectQuery(q)
	iter := es.session.Query(query).Iter()
	return es.getFullEvents(iter)
}

func (es *EventStore) GetTopics() []string {
	iter := es.session.Query("SELECT topic_name FROM event_topics;").Iter()
	var topic string
	var topics []string
	for true {
		if iter.Scan(&topic) {
			topics = append(topics, topic)
		} else {
			break
		}
	}
	return topics
}

func (es *EventStore) GetDcs() []string {
	iter := es.session.Query("SELECT dc FROM event_dcs;").Iter()
	var dc string
	var dcs []string
	for true {
		if iter.Scan(&dc) {
			dcs = append(dcs, dc)
		} else {
			break
		}
	}
	return dcs
}

func (es *EventStore) CloseSession() {
	es.session.Close()
}
