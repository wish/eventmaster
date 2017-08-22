package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	eventmaster "github.com/ContextLogic/eventmaster/proto"
	cass "github.com/ContextLogic/eventmaster/src/cassandra_client"
	servicelookup "github.com/ContextLogic/goServiceLookup/servicelookup"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

type streamFn func(eventId string) error

type DataStore interface {
	AddEvent(*Event) error
	Find(q *eventmaster.Query, topicIds []string, dcIds []string) (Events, error)
	FindById(string, bool) (*Event, error)
	FindIds(*eventmaster.TimeQuery, streamFn) error
	GetTopics() ([]Topic, error)
	AddTopic(RawTopic) error
	UpdateTopic(RawTopic) error
	DeleteTopic(string) error
	GetDcs() ([]Dc, error)
	AddDc(Dc) error
	UpdateDc(string, string) error
	CloseSession()
}

type CassandraConfig struct {
	Addr        []string `json:"addr"`
	Keyspace    string   `json:"keyspace"`
	Consistency string   `json:"consistency"`
	Timeout     string   `json:"timeout"`
	ServiceName string   `json:"service_name"`
}

type CassandraStore struct {
	session cass.Session
}

func NewCassandraStore(c CassandraConfig) (*CassandraStore, error) {
	var cassandraIps []string

	if c.ServiceName != "" {
		slClient := servicelookup.NewClient(false)
		cassandraIps = slClient.GetServiceIps(c.ServiceName, "")
	} else {
		cassandraIps = c.Addr
	}

	fmt.Println("Connecting to cassandra:", cassandraIps)
	session, err := cass.NewCqlSession(cassandraIps, c.Keyspace, c.Consistency, c.Timeout)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating cassandra session")
	}

	return &CassandraStore{
		session: session,
	}, nil
}

func (event *Event) toCassandra() (string, error) {
	date := getDate(event.EventTime / 1000)
	data := "{}"
	if event.Data != nil {
		dataBytes, err := json.Marshal(event.Data)
		if err != nil {
			return "", errors.Wrap(err, "Error marshalling event data into json")
		}
		data = string(dataBytes)
	}
	coreFields := fmt.Sprintf(`
    INSERT INTO event (event_id, parent_event_id, dc_id, topic_id, host, target_host_set, user, event_time, tag_set, received_time, date)
    VALUES (%[1]s, %[2]s, %[3]s, %[4]s, %[5]s, %[6]s, %[7]s, %[8]d, %[9]s, %[11]d, %[12]s);
    INSERT INTO event_metadata(event_id, data_json)
    VALUES (%[1]s, $$%[10]s$$);
    INSERT INTO event_by_topic(event_id, topic_id, event_time, date)
    VALUES (%[1]s, %[4]s, %[8]d, %[12]s);
    INSERT INTO event_by_dc(event_id, dc_id, event_time, date)
    VALUES (%[1]s, %[3]s, %[8]d, %[12]s);
    INSERT INTO event_by_host(event_id, host, event_time, date)
    VALUES (%[1]s, %[5]s, %[8]d, %[12]s);
    INSERT INTO event_by_date(event_id, event_time, date)
    VALUES (%[1]s, %[8]d, %[12]s);`,
		stringify(event.EventID), stringify(event.ParentEventID), stringifyUUID(event.DcID), stringifyUUID(event.TopicID),
		stringify(strings.ToLower(event.Host)), stringifyArr(event.TargetHosts), stringify(strings.ToLower(event.User)), event.EventTime,
		stringifyArr(event.Tags), data, event.ReceivedTime, stringify(date))
	userField := ""
	parentEventIdField := ""
	if event.User != "" {
		userField = fmt.Sprintf(`INSERT INTO event_by_user(event_id, user, event_time, date)
		VALUES (%s, %s, %d, %s);`, stringify(event.EventID), stringify(event.User), event.EventTime, stringify(date))
	}
	if event.ParentEventID != "" {
		parentEventIdField = fmt.Sprintf(`INSERT INTO event_by_parent_event_id(event_id, parent_event_id, event_time, date)
		VALUES (%s, %s, %d, %s);`, stringify(event.EventID), stringify(event.ParentEventID), event.EventTime, stringify(date))
	}

	return fmt.Sprintf(`
		BEGIN BATCH
		%s
		%s
		%s
		APPLY BATCH;`, coreFields, userField, parentEventIdField), nil
}

func (c *CassandraStore) AddEvent(evt *Event) error {
	query, err := evt.toCassandra()
	if err != nil {
		return errors.Wrap(err, "Error converting event to cassandra event")
	}
	return c.session.ExecQuery(query)
}

func (c *CassandraStore) getFromTable(tableName string, columnName string, dates []string, timeFilter string, fields []string) (map[string]struct{}, error) {
	events := make(map[string]struct{})
	var eventID string
	for _, date := range dates {
		dateFilter := fmt.Sprintf("date = %s", stringify(date))
		// TODO: put a reasonable limit on this
		query := fmt.Sprintf(`SELECT event_id FROM %s WHERE %s in (%s) AND %s AND %s LIMIT 200;`,
			tableName, columnName, strings.Join(fields, ","), dateFilter, timeFilter)
		scanIter, closeIter := c.session.ExecIterQuery(query)
		for true {
			if scanIter(&eventID) {
				events[eventID] = v
			} else {
				break
			}
		}
		if err := closeIter(); err != nil {
			return nil, errors.Wrap(err, "Error closing cassandra iter")
		}
	}

	return events, nil
}

func (c *CassandraStore) joinEvents(evts map[string]struct{}, newEvts map[string]struct{}, needsIntersection bool) map[string]struct{} {
	if needsIntersection {
		intersection := make(map[string]struct{})
		for eID, _ := range newEvts {
			if _, ok := evts[eID]; ok {
				intersection[eID] = v
			}
		}
		return intersection
	} else {
		return newEvts
	}
}

func (c *CassandraStore) FindById(id string, includeData bool) (*Event, error) {
	var topicID, dcID gocql.UUID
	var eventTime, receivedTime int64
	var eventID, parentEventID, host, user string
	var targetHostSet, tagSet []string
	var evt *Event
	scanIter, closeIter := c.session.ExecIterQuery(
		fmt.Sprintf(`SELECT event_id, dc_id, event_time, host, parent_event_id, received_time, tag_set, target_host_set, topic_id, user
			FROM event WHERE event_id=%s LIMIT 1;`, stringify(id)))
	if scanIter(&eventID, &dcID, &eventTime, &host, &parentEventID, &receivedTime, &tagSet, &targetHostSet, &topicID, &user) {
		evt = &Event{
			EventID:       eventID,
			ParentEventID: parentEventID,
			EventTime:     eventTime / 1000,
			DcID:          dcID.String(),
			TopicID:       topicID.String(),
			Tags:          tagSet,
			Host:          host,
			TargetHosts:   targetHostSet,
			User:          user,
			ReceivedTime:  receivedTime,
		}
	}
	if err := closeIter(); err != nil {
		return nil, err
	}
	if includeData {
		var data string
		scanIter, closeIter := c.session.ExecIterQuery(
			fmt.Sprintf(`SELECT data_json
			FROM event_metadata WHERE event_id=%s LIMIT 1;`, stringify(id)))
		if scanIter(&data) {
			var d map[string]interface{}
			if data != "" {
				if err := json.Unmarshal([]byte(data), &d); err != nil {
					return nil, errors.Wrap(err, "Error unmarshalling JSON in event data")
				}
			}
			evt.Data = d
		}
		if err := closeIter(); err != nil {
			return nil, err
		}
	}
	return evt, nil
}

func getDates(startEventTime int64, endEventTime int64) ([]string, error) {
	var dates []string
	startDate := getDate(startEventTime)
	endDate := getDate(endEventTime)

	for {
		if startDate == endDate {
			break
		} else {
			dates = append(dates, endDate)
			nextDate, err := time.Parse("2006-01-02", endDate)
			if err != nil {
				return nil, errors.Wrap(err, "Error parsing start date")
			}
			endDate = getDate(nextDate.Add(time.Hour * -24).Unix())
		}
	}
	dates = append(dates, startDate)
	return dates, nil
}

func (c *CassandraStore) Find(q *eventmaster.Query, topicIds []string, dcIds []string) (Events, error) {
	dates, err := getDates(q.StartEventTime, q.EndEventTime)
	if err != nil {
		return nil, errors.Wrap(err, "Error getting dates from timestamps")
	}
	timeFilter := fmt.Sprintf("event_time >= %d AND event_time <= %d", q.StartEventTime*1000, q.EndEventTime*1000)

	needsIntersection := false
	evts := make(map[string]struct{})
	if len(q.User) > 0 {
		var users []string
		for _, user := range q.User {
			users = append(users, stringify(strings.ToLower(user)))
		}
		userEvts, err := c.getFromTable("event_by_user", "user", dates, timeFilter, users)
		if err != nil {
			return nil, errors.Wrap(err, "Error getting event ids from cassandra table")
		}
		evts = c.joinEvents(evts, userEvts, needsIntersection)
		if len(evts) == 0 {
			return nil, nil
		}
		needsIntersection = true
	}
	if len(q.ParentEventId) > 0 {
		var parentEventIds []string
		for _, peId := range q.ParentEventId {
			parentEventIds = append(parentEventIds, stringify(peId))
		}
		peIdEvts, err := c.getFromTable("event_by_parent_event_id", "parent_event_id", dates, timeFilter, parentEventIds)
		if err != nil {
			return nil, errors.Wrap(err, "Error getting event ids from cassandra table")
		}
		evts = c.joinEvents(evts, peIdEvts, needsIntersection)
		if len(evts) == 0 {
			return nil, nil
		}
		needsIntersection = true
	}
	if len(q.Host) > 0 {
		var hosts []string
		for _, host := range q.Host {
			hosts = append(hosts, stringify(strings.ToLower(host)))
		}
		hostEvts, err := c.getFromTable("event_by_host", "host", dates, timeFilter, hosts)
		if err != nil {
			return nil, errors.Wrap(err, "Error getting event ids from cassandra table")
		}
		evts = c.joinEvents(evts, hostEvts, needsIntersection)
		if len(evts) == 0 {
			return nil, nil
		}
		needsIntersection = true
	}
	if len(topicIds) > 0 {
		topicEvts, err := c.getFromTable("event_by_topic", "topic_id", dates, timeFilter, topicIds)
		if err != nil {
			return nil, errors.Wrap(err, "Error getting event ids from cassandra table")
		}
		evts = c.joinEvents(evts, topicEvts, needsIntersection)
		if len(evts) == 0 {
			return nil, nil
		}
		needsIntersection = true
	}
	if len(dcIds) > 0 {
		dcEvts, err := c.getFromTable("event_by_dc", "dc_id", dates, timeFilter, dcIds)
		if err != nil {
			return nil, errors.Wrap(err, "Error getting event ids from cassandra table")
		}
		evts = c.joinEvents(evts, dcEvts, needsIntersection)
		if len(evts) == 0 {
			return nil, nil
		}
		needsIntersection = true
	}
	if !needsIntersection {
		for _, date := range dates {
			var eventID string
			dateFilter := fmt.Sprintf("date = %s", stringify(date))
			query := fmt.Sprintf(`SELECT event_id FROM %s WHERE %s AND %s LIMIT 200;`,
				"event_by_date", dateFilter, timeFilter)
			scanIter, closeIter := c.session.ExecIterQuery(query)
			for true {
				if scanIter(&eventID) {
					evts[eventID] = v
				} else {
					break
				}
			}
			if err := closeIter(); err != nil {
				return nil, errors.Wrap(err, "Error closing cassandra iter")
			}
		}
	}

	ch := make(chan *Event, len(evts))
	for eID, _ := range evts {
		go func(eID string) {
			evt, err := c.FindById(eID, false)
			if err != nil {
				fmt.Println("Error closing cassandra iter on read:", err)
				ch <- nil
			} else {
				ch <- evt
			}
		}(eID)
	}

	eventMap := make(map[string]*Event)
	for _, _ = range evts {
		if evt := <-ch; evt != nil {
			eventMap[evt.EventID] = evt
		}
	}

	// filter based on target host and tags
	if len(q.TargetHostSet) > 0 {
		targetHosts := make(map[string]struct{})
		for _, thost := range q.TargetHostSet {
			targetHosts[thost] = v
		}
		for eID, evtData := range eventMap {
			exists := false
			for _, th := range evtData.TargetHosts {
				if _, ok := targetHosts[th]; ok {
					exists = true
				}
			}
			if !exists {
				delete(eventMap, eID)
			}
		}
	}

	if len(q.TagSet) > 0 || len(q.ExcludeTagSet) > 0 {
		tags := make(map[string]struct{})
		for _, tag := range q.TagSet {
			tags[tag] = v
		}
		excludeTags := make(map[string]struct{})
		for _, tag := range q.ExcludeTagSet {
			excludeTags[tag] = v
		}
		andOp := q.TagAndOperator
		for eID, evtData := range eventMap {
			deleteEvt := false
			if andOp {
				for tag, _ := range tags {
					tagExists := false
					for _, t := range evtData.Tags {
						if tag == t {
							tagExists = true
							break
						}
					}
					if !tagExists {
						deleteEvt = true
						break
					}
				}
			} else {
				missingTags := true
				for _, t := range evtData.Tags {
					if _, ok := tags[t]; ok {
						missingTags = false
						break
					}
				}
				deleteEvt = missingTags
			}

			if !deleteEvt && len(q.ExcludeTagSet) > 0 {
				for _, t := range evtData.Tags {
					if _, ok := excludeTags[t]; ok {
						deleteEvt = true
						break
					}
				}
			}

			if deleteEvt {
				delete(eventMap, eID)
			}
		}
	}

	var events []*Event
	for _, event := range eventMap {
		events = append(events, event)
	}
	return events, nil
}

func (c *CassandraStore) FindIds(q *eventmaster.TimeQuery, stream streamFn) error {
	dates, err := getDates(q.StartEventTime, q.EndEventTime)
	if err != nil {
		return errors.Wrap(err, "Error getting dates from start and end time")
	}
	timeFilter := fmt.Sprintf("event_time >= %d AND event_time <= %d", q.StartEventTime*1000, q.EndEventTime*1000)
	for _, date := range dates {
		var eventID string
		dateFilter := fmt.Sprintf("date = %s", stringify(date))
		order := "DESC"
		if q.Ascending {
			order = "ASC"
		}
		query := fmt.Sprintf(`SELECT event_id FROM %s WHERE %s AND %s ORDER BY event_time %s LIMIT %d;`,
			"event_by_date", dateFilter, timeFilter, order, q.Limit)
		scanIter, closeIter := c.session.ExecIterQuery(query)
		for scanIter(&eventID) {
			if err := stream(eventID); err != nil {
				closeIter()
				return errors.Wrap(err, "Error streaming event ID")
			}
		}
		if err := closeIter(); err != nil {
			return errors.Wrap(err, "Error closing cassandra iter")
		}
	}
	return nil
}

func (c *CassandraStore) GetTopics() ([]Topic, error) {
	scanIter, closeIter := c.session.ExecIterQuery("SELECT topic_id, topic_name, data_schema FROM event_topic;")
	var topicId gocql.UUID
	var name, schema string
	var topics []Topic
	for {
		if scanIter(&topicId, &name, &schema) {
			var s map[string]interface{}
			err := json.Unmarshal([]byte(schema), &s)
			if err != nil {
				return nil, errors.Wrap(err, "Error unmarshalling schema")
			}
			topics = append(topics, Topic{
				ID:     topicId.String(),
				Name:   name,
				Schema: s,
			})
		} else {
			break
		}
	}
	if err := closeIter(); err != nil {
		return nil, errors.Wrap(err, "Error closing iter")
	}
	return topics, nil
}

func (c *CassandraStore) AddTopic(t RawTopic) error {
	queryStr := fmt.Sprintf(`INSERT INTO event_topic
		(topic_id, topic_name, data_schema)
		VALUES (%[1]s, %[2]s, %[3]s);`,
		t.ID, stringify(t.Name), stringify(t.Schema))

	return c.session.ExecQuery(queryStr)
}

func (c *CassandraStore) UpdateTopic(t RawTopic) error {
	queryStr := fmt.Sprintf(`UPDATE event_topic SET
		topic_name=%s,
		data_schema=%s
		WHERE topic_id=%s;`, stringify(t.Name), stringify(t.Schema), t.ID)
	return c.session.ExecQuery(queryStr)
}

func (c *CassandraStore) DeleteTopic(id string) error {
	return c.session.ExecQuery(fmt.Sprintf(`DELETE FROM event_topic WHERE topic_id=%[1]s;`,
		id))
}

func (c *CassandraStore) GetDcs() ([]Dc, error) {
	scanIter, closeIter := c.session.ExecIterQuery("SELECT dc_id, dc FROM event_dc;")
	var id gocql.UUID
	var dc string
	var dcs []Dc
	for true {
		if scanIter(&id, &dc) {
			dcs = append(dcs, Dc{
				ID:   id.String(),
				Name: dc,
			})
		} else {
			break
		}
	}
	if err := closeIter(); err != nil {
		return nil, errors.Wrap(err, "Error closing iter")
	}
	return dcs, nil
}

func (c *CassandraStore) AddDc(dc Dc) error {
	queryStr := fmt.Sprintf(`INSERT INTO event_dc 
		(dc_id, dc)
		VALUES (%[1]s, %[2]s);`,
		dc.ID, stringify(dc.Name))

	return c.session.ExecQuery(queryStr)
}

func (c *CassandraStore) UpdateDc(id string, newName string) error {
	queryStr := fmt.Sprintf(`UPDATE event_dc SET dc=%s WHERE dc_id=%s;`,
		stringify(newName), id)
	return c.session.ExecQuery(queryStr)
}

func (c *CassandraStore) CloseSession() {
	c.session.Close()
}
