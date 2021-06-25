package eventmaster

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

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
		" VALUES ($1, $2, $3, $4, $5, $6, $7, to_timestamp($8), $9, to_timestamp($10))",
		e.EventID, e.ParentEventID, e.DCID, e.TopicID, strings.ToLower(e.Host), pq.Array(e.TargetHosts), strings.ToLower(e.User), e.EventTime/1000, pq.Array(e.Tags), e.ReceivedTime/1000)

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

func (p *PostgresStore) Find(q *eventmaster.Query, topicIDs []string, dcIDs []string) (Events, error) {
	// TODO: implement this function
	return nil, nil
}

func (p *PostgresStore) FindByID(string, bool) (*Event, error) {
	// TODO: implement this function
	return nil, nil
}

func (p *PostgresStore) FindIDs(*eventmaster.TimeQuery, HandleEvent) error {
	// TODO: implement this function
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
