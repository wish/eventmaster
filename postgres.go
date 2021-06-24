package eventmaster

import (
	"database/sql"
	"fmt"

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

func (p *PostgresStore) AddEvent(*Event) error {
	// TODO: implement this function
	return nil
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
	// TODO: implement this function
	return nil, nil
}

func (p *PostgresStore) AddTopic(RawTopic) error {
	// TODO: implement this function
	return nil
}

func (p *PostgresStore) UpdateTopic(RawTopic) error {
	// TODO: implement this function
	return nil
}

func (p *PostgresStore) DeleteTopic(string) error {
	// TODO: implement this function
	return nil
}

func (p *PostgresStore) GetDCs() ([]DC, error) {
	// TODO: implement this function
	return nil, nil
}

func (p *PostgresStore) AddDC(DC) error {
	// TODO: implement this function
	return nil
}

func (p *PostgresStore) UpdateDC(string, string) error {
	// TODO: implement this function
	return nil
}

func (p *PostgresStore) CloseSession() {
	// TODO: implement this function
	return
}
