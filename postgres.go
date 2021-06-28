package eventmaster

import (
	"database/sql"
	"encoding/json"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	eventmaster "github.com/wish/eventmaster/proto"
)

// implements datastore
type PostgresStore struct {
	db *sql.DB
}

type PostgresConfig struct {
	Addr        string `json:"addr"`
	Port        int    `json:"port"`
	Database    string `json:"database"`
	ServiceName string `json:"service_name"`
	Username    string `json:"username"`
	Password    string `json:"password"`
}

func NewPostgresStore(c PostgresConfig) (*PostgresStore, error) {
	var host string
	if c.ServiceName != "" {
		host = c.ServiceName + ".service.consul"
	} else {
		host = c.Addr
	}
	log.Infof("Connecting to postgres: %v", host)
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, c.Port, c.Username, c.Password, c.Database)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, errors.Wrap(err, "Error creating postgres session")
	}
	err = db.Ping()
	if err != nil {
		return nil, errors.Wrap(err, "Error creating postgres session")
	}
	log.Infof("Successfully connected to postgres %s", host)

	return &PostgresStore{
		db: db,
	}, nil
}

func (p *PostgresStore) AddEvent(e *Event) error {
	data := "{}"
	if e.Data != nil {
		dataBytes, err := json.Marshal(e.Data)
		if err != nil {
			return err
		}
		data = string(dataBytes)
	}

	tx, err := p.db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec("INSERT INTO event "+
		"(event_id, parent_event_id, dc_id, topic_id, host, target_host_set, \"user\", event_time, tag_set, received_time)"+
		" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		e.EventID, e.ParentEventID, e.DCID, e.TopicID, e.Host, pq.Array(e.TargetHosts), e.User, e.EventTime, pq.Array(e.Tags), e.ReceivedTime)

	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("INSERT INTO event_metadata (event_id, data_json) VALUES ($1, $2)", e.EventID, data)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func getPlaceHolderArray(slice []string) string {
	expr := "ARRAY["
	first := true
	for i := 0; i < len(slice); i++ {
		if !first {
			expr += ", "
		}
		expr += "?"
		first = false
	}
	expr += "]"
	return expr
}

func stringsToIfaces(in []string) []interface{} {
	new := make([]interface{}, len(in))
	for i, v := range in {
		new[i] = v
	}
	return new
}

func (p *PostgresStore) Find(q *eventmaster.Query, topicIDs []string, dcIDs []string) (Events, error) {
	// use sql builder for better readability
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	columns := []string{"event_id", "parent_event_id", "dc_id", "topic_id", "host", "target_host_set", "\"user\"", "event_time", "tag_set", "received_time"}
	table := "event"
	base_query := psql.Select(columns...).From(table)

	wheres := sq.And{}

	// add constarints, *1000 due to different time precision (sec to ms)
	wheres = append(wheres, sq.GtOrEq{"event_time": q.StartEventTime * 1000})
	wheres = append(wheres, sq.LtOrEq{"event_time": q.EndEventTime * 1000})

	if len(q.User) > 0 {
		wheres = append(wheres, sq.Eq{"user": q.User})
	}

	if len(q.ParentEventID) > 0 {
		wheres = append(wheres, sq.Eq{"parent_event_id": q.ParentEventID})
	}

	if len(q.Host) > 0 {
		wheres = append(wheres, sq.Eq{"host": q.Host})
	}

	if len(topicIDs) > 0 {
		wheres = append(wheres, sq.Eq{"topic_id": topicIDs})
	}

	if len(dcIDs) > 0 {
		wheres = append(wheres, sq.Eq{"dc_id": dcIDs})
	}

	if len(q.TargetHostSet) > 0 {
		// intersection of two arrays
		// squirrel doesn't support Postgres' array and repective methods. Building new SQL template manually
		// make identical slice of []interface{} type to pass to varadic functions
		clause := "target_host_set && " + getPlaceHolderArray((q.TargetHostSet))
		wheres = append(wheres, sq.Expr(clause, stringsToIfaces(q.TargetHostSet)...))
	}

	if len(q.TagSet) > 0 {
		var op string
		if q.TagAndOperator {
			// LHS array is the superset of RHS array
			op = ">&"
		} else {
			// LHS array has interestion with RHS array
			op = "&&"
		}
		clause := fmt.Sprintf("tag_set %s %s", op, getPlaceHolderArray(q.TagSet))
		wheres = append(wheres, sq.Expr(clause, stringsToIfaces(q.TagSet)...))
	}

	if len(q.ExcludeTagSet) > 0 {
		clause := fmt.Sprintf("NOT (tag_set && %s)", getPlaceHolderArray((q.ExcludeTagSet)))
		wheres = append(wheres, sq.Expr(clause, stringsToIfaces(q.ExcludeTagSet)...))
	}

	query := base_query.Where(wheres)

	var events []*Event

	var event_id string
	var parent_event_id string
	var dc_id string
	var topic_id string
	var host string
	var target_host_set []string
	var user string
	var event_time int64
	var tag_set []string
	var received_time int64

	rows, err := query.RunWith(p.db).Query()
	for rows.Next() {
		err = rows.Scan(&event_id, &parent_event_id, &dc_id, &topic_id, &host, pq.Array(&target_host_set), &user, &event_time, pq.Array(&tag_set), &received_time)
		if err != nil {
			return nil, err
		}
		events = append(events, &Event{
			EventID:       event_id,
			ParentEventID: parent_event_id,
			DCID:          dc_id,
			TopicID:       topic_id,
			Host:          host,
			TargetHosts:   target_host_set,
			User:          user,
			EventTime:     event_time,
			Tags:          tag_set,
			ReceivedTime:  received_time,
		})
	}

	return events, nil
}

func (p *PostgresStore) FindByID(id string, inclData bool) (*Event, error) {
	rows, err := p.db.Query("SELECT event_id, parent_event_id, dc_id, topic_id, host,"+
		" target_host_set, user, event_time, tag_set, received_time "+
		"FROM event WHERE event_id=$1 LIMIT 1", id)

	if err != nil {
		return nil, err
	}

	event := Event{}
	if rows.Next() {
		err = rows.Scan(&event.EventID, &event.ParentEventID, &event.DCID,
			&event.TopicID, &event.Host, pq.Array(&event.TargetHosts), &event.User,
			&event.EventTime, pq.Array(&event.Tags), &event.ReceivedTime)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("cannot find event entry with given id")
	}

	if err != nil {
		return nil, err
	}

	if inclData {
		rows, err = p.db.Query("SELECT data_json FROM event_metadata WHERE event_id=$1 LIMIT 1", id)
		if err != nil {
			return nil, err
		}
		var data []byte
		if rows.Next() {
			err = rows.Scan(&data)
			if err != nil {
				return nil, err
			}
			err = json.Unmarshal(data, &event.Data)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, errors.New("cannot find data entry with given id")
		}
	}

	return &event, nil
}

func (p *PostgresStore) FindIDs(query *eventmaster.TimeQuery, handle HandleEvent) error {
	var order string
	if query.Ascending {
		order = "ASC"
	} else {
		order = "DESC"
	}

	// *1000 to convert time precision (sec to ms)
	rows, err := p.db.Query("SELECT event_id FROM event WHERE event_time >= $1 AND event_time <= $2 "+
		"ORDER BY event_time "+order+" LIMIT $3 ", query.StartEventTime*1000, query.EndEventTime*1000, query.Limit)
	if err != nil {
		return err
	}

	for rows.Next() {
		var id string
		err = rows.Scan(&id)
		if err != nil {
			return err
		}
		err = handle(id)
		if err != nil {
			rows.Close()
			return err
		}
	}
	return nil
}

func (p *PostgresStore) GetTopics() ([]Topic, error) {
	rows, err := p.db.Query("SELECT topic_id, topic_name, data_schema FROM event_topic")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var topics []Topic
	var id string
	var name, schema string

	for rows.Next() {
		err = rows.Scan(&id, &name, &schema)
		if err != nil {
			return nil, err
		}
		var s map[string]interface{}
		err := json.Unmarshal([]byte(schema), &s)
		if err != nil {
			return nil, err
		}
		topics = append(topics, Topic{
			ID:     id,
			Name:   name,
			Schema: s,
		})
	}

	return topics, nil
}

func (p *PostgresStore) AddTopic(t RawTopic) error {
	_, err := p.db.Exec("INSERT INTO event_topic (topic_id, topic_name, data_schema) VALUES ($1, $2, $3)", t.ID, t.Name, t.Schema)
	return err
}

func (p *PostgresStore) UpdateTopic(t RawTopic) error {
	_, err := p.db.Exec("UPDATE event_topic SET topic_name=$1, data_schema=$2 WHERE topic_id=$3", t.Name, t.Schema, t.ID)
	return err
}

func (p *PostgresStore) DeleteTopic(id string) error {
	_, err := p.db.Exec("DELETE FROM event_topic WHERE topic_id=$1", id)
	return err
}

func (p *PostgresStore) GetDCs() ([]DC, error) {
	rows, err := p.db.Query("SELECT dc_id, dc from event_dc")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var dcs []DC

	var dc_id, dc string
	for rows.Next() {
		err = rows.Scan(&dc_id, &dc)
		if err != nil {
			return nil, err
		}
		dcs = append(dcs, DC{
			ID:   dc_id,
			Name: dc,
		})
	}
	return dcs, nil
}

func (p *PostgresStore) AddDC(dc DC) error {
	stmt, err := p.db.Prepare("INSERT INTO event_dc (dc_id, dc) VALUES ($1, $2)")
	if err != nil {
		return errors.Wrap(err, "Failed preparing insert DC statement")
	}
	_, err = stmt.Exec(dc.ID, dc.Name)
	if err != nil {
		return err
	}

	return nil
}

func (p *PostgresStore) UpdateDC(id string, newName string) error {
	_, err := p.db.Exec("UPDATE event_dc SET dc=$1 WHERE dc_id=$2", newName, id)
	return err
}

func (p *PostgresStore) CloseSession() {
	p.db.Close()
}
