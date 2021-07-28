package eventmaster

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	vaultApi "github.com/hashicorp/vault/api"
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
type VaultConfig struct {
	Enabled bool   `json:"enabled"`
	Addr    string `json:"addr"`
	Token   string `json:"token"`
	Path    string `json:"path"`
}

type PostgresConfig struct {
	Addr        string      `json:"addr"`
	Port        int         `json:"port"`
	Database    string      `json:"database"`
	ServiceName string      `json:"service_name"`
	Username    string      `json:"username"`
	Password    string      `json:"password"`
	Vault       VaultConfig `json:"vault"`
}

func getPasswordFromVault(c VaultConfig) (*string, error) {
	config := &vaultApi.Config{
		Address: c.Addr,
	}
	client, err := vaultApi.NewClient(config)
	if err != nil {
		return nil, err
	}

	client.SetToken(c.Token)
	secret, err := client.Logical().Read(c.Path)
	if err != nil {
		return nil, err
	}

	m, ok := secret.Data["data"].(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("Vault data malformed")
	}

	ret, ok := m["password"].(string)
	if !ok {
		return nil, errors.Errorf("Secret format malformed")
	}
	return &ret, nil
}

func getPassword(c PostgresConfig) (*string, error) {
	var password *string
	var err error
	if !c.Vault.Enabled {
		password = &c.Password
	} else {
		password, err = getPasswordFromVault(c.Vault)
		if err != nil {
			return nil, err
		}
	}
	return password, nil
}

// Initializes new connection to Postgres DB
func NewPostgresStore(c PostgresConfig) (*PostgresStore, error) {
	var host string
	if c.ServiceName != "" {
		host = c.ServiceName + ".service.consul"
	} else {
		host = c.Addr
	}
	log.Infof("Connecting to postgres: %v", host)
	password, err := getPassword(c)
	if err != nil {
		return nil, err
	}
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, c.Port, c.Username, *password, c.Database)
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

// Add a new event to the DB
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

	// e.EventTime and e.ReceivedTime here is Unix timestamp in ms precision
	_, err = tx.Exec("INSERT INTO event "+
		"(event_id, parent_event_id, dc_id, topic_id, host, target_host_set, \"user\", event_time, tag_set, received_time)"+
		" VALUES ($1, $2, $3, $4, $5, $6, $7, to_timestamp($8) AT TIME ZONE 'UTC', $9, to_timestamp($10) AT TIME ZONE 'UTC')",
		e.EventID, e.ParentEventID, e.DCID, e.TopicID, e.Host, pq.Array(e.TargetHosts), e.User, e.EventTime/1000, pq.Array(e.Tags), e.ReceivedTime/1000)

	if err != nil {
		rollBackErr := tx.Rollback()
		if rollBackErr != nil {
			return errors.Wrap(err, "Rollback failure")
		}
		return err
	}

	_, err = tx.Exec("INSERT INTO event_metadata (event_id, data_json) VALUES ($1, $2)", e.EventID, data)
	if err != nil {
		rollBackErr := tx.Rollback()
		if rollBackErr != nil {
			return errors.Wrap(err, "Rollback failure")
		}
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

// Find respective events that match given criteria
func (p *PostgresStore) Find(q *eventmaster.Query, topicIDs []string, dcIDs []string, inclData bool) (Events, error) {
	// use sql builder for better readability
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	columns := []string{"event_id", "parent_event_id", "dc_id", "topic_id", "host", "target_host_set", "\"user\"", "event_time", "tag_set", "received_time"}
	if inclData {
		columns = append(columns, "data_json")
	}
	base_query := psql.Select(columns...).From("event")
	if inclData {
		base_query = base_query.Join("event_metadata USING (event_id)")
	}

	wheres := sq.And{}

	wheres = append(wheres, sq.GtOrEq{"event_time": time.Unix(q.StartEventTime, 0)})
	wheres = append(wheres, sq.LtOrEq{"event_time": time.Unix(q.EndEventTime, 0)})

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
			op = "@>"
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
	var event_time time.Time
	var tag_set []string
	var received_time time.Time
	data := "{}"

	fields := make([]interface{}, 0)
	fields = append(fields, &event_id, &parent_event_id, &dc_id, &topic_id, &host, pq.Array(&target_host_set), &user, &event_time, pq.Array(&tag_set), &received_time)
	if inclData {
		fields = append(fields, &data)
	}

	rows, err := query.RunWith(p.db).Query()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		err = rows.Scan(fields...)
		if err != nil {
			return nil, err
		}
		event := &Event{
			EventID:       event_id,
			ParentEventID: parent_event_id,
			DCID:          dc_id,
			TopicID:       topic_id,
			Host:          host,
			TargetHosts:   target_host_set,
			User:          user,
			EventTime:     event_time.Unix(),
			Tags:          tag_set,
			ReceivedTime:  received_time.Unix(),
		}
		err = json.Unmarshal([]byte(data), &event.Data)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, nil
}

// Find the event with given ID, include detailed json data if needed
func (p *PostgresStore) FindByID(id string, inclData bool) (*Event, error) {
	rows, err := p.db.Query("SELECT event_id, parent_event_id, dc_id, topic_id, host,"+
		" target_host_set, user, event_time, tag_set, received_time "+
		"FROM event WHERE event_id=$1 LIMIT 1", id)

	if err != nil {
		return nil, err
	}

	event := Event{}
	if rows.Next() {
		var receivedTime, eventTime time.Time
		err = rows.Scan(&event.EventID, &event.ParentEventID, &event.DCID,
			&event.TopicID, &event.Host, pq.Array(&event.TargetHosts), &event.User,
			&eventTime, pq.Array(&event.Tags), &receivedTime)
		if err != nil {
			return nil, err
		}
		event.ReceivedTime = receivedTime.Unix()
		event.EventTime = eventTime.Unix()
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

// Find eventIDs that match the given TimeQuery constraints
func (p *PostgresStore) FindIDs(query *eventmaster.TimeQuery, handle HandleEvent) error {
	var order string
	if query.Ascending {
		order = "ASC"
	} else {
		order = "DESC"
	}

	// query.StartEventTime and query.EndEventTime is in sec precision here
	rows, err := p.db.Query("SELECT event_id FROM event WHERE event_time >= to_timestamp($1) AT TIME ZONE 'UTC'"+
		" AND event_time <= to_timestamp($2) AT TIME ZONE 'UTC' ORDER BY event_time "+
		order+" LIMIT $3 ", query.StartEventTime, query.EndEventTime, query.Limit)
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

// Get all topics
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

// Add a topic to the DB
func (p *PostgresStore) AddTopic(t RawTopic) error {
	_, err := p.db.Exec("INSERT INTO event_topic (topic_id, topic_name, data_schema) VALUES ($1, $2, $3)", t.ID, t.Name, t.Schema)
	return err
}

// Update topic with given topic ID
func (p *PostgresStore) UpdateTopic(t RawTopic) error {
	_, err := p.db.Exec("UPDATE event_topic SET topic_name=$1, data_schema=$2 WHERE topic_id=$3", t.Name, t.Schema, t.ID)
	return err
}

// Delete topic with given topic ID
func (p *PostgresStore) DeleteTopic(id string) error {
	_, err := p.db.Exec("DELETE FROM event_topic WHERE topic_id=$1", id)
	return err
}

// Get all Datacenters
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

// Add a datacenter to the table
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

// Update datacenter with given ID
func (p *PostgresStore) UpdateDC(id string, newName string) error {
	_, err := p.db.Exec("UPDATE event_dc SET dc=$1 WHERE dc_id=$2", newName, id)
	return err
}

// Close the database session
func (p *PostgresStore) CloseSession() {
	p.db.Close()
}
