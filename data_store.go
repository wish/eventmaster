package eventmaster

import (
	eventmaster "github.com/wish/eventmaster/proto"
)

// DataStore defines the interface needed to be used as a backing store for
// eventmaster.
//
// A few examples include CassandraStore and MockDataStore.
type DataStore interface {
	AddEvent(*Event) error
	Find(q *eventmaster.Query, topicIDs []string, dcIDs []string, inclData bool) (Events, error)
	FindByID(string, bool) (*Event, error)
	FindIDs(*eventmaster.TimeQuery, HandleEvent) error
	GetTopics() ([]Topic, error)
	AddTopic(RawTopic) error
	UpdateTopic(RawTopic) error
	DeleteTopic(string) error
	GetDCs() ([]DC, error)
	AddDC(DC) error
	UpdateDC(string, string) error
	CloseSession()
}

// HandleEvent defines a function for interacting with a stream of events one
// at a time.
type HandleEvent func(eventID string) error
