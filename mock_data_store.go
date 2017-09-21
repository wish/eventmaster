package eventmaster

import (
	"github.com/pkg/errors"

	proto "github.com/ContextLogic/eventmaster/proto"
)

type MockDataStore struct {
	events []*Event

	dcs    []Dc
	topics []Topic
}

func (mds *MockDataStore) AddEvent(e *Event) error {
	mds.events = append(mds.events, e)
	return nil
}

func (mds *MockDataStore) Find(q *proto.Query, topicImds []string, dcImds []string) (Events, error) {
	return nil, errors.New("NYI")
}

func (mds *MockDataStore) FindById(id string, data bool) (*Event, error) {
	return nil, errors.New("NYI")
}

func (mds *MockDataStore) FindIds(*proto.TimeQuery, streamFn) error {
	return errors.New("NYI")
}

func (mds *MockDataStore) GetTopics() ([]Topic, error) {
	return mds.topics, nil
}

func (mds *MockDataStore) AddTopic(rt RawTopic) error {
	mds.topics = append(mds.topics, Topic{ID: rt.ID, Name: rt.Name})
	return nil
}

func (mds *MockDataStore) UpdateTopic(rt RawTopic) error {
	return errors.New("NYI")
}

func (mds *MockDataStore) DeleteTopic(string) error {
	return errors.New("NYI")
}

func (mds *MockDataStore) GetDcs() ([]Dc, error) {
	return mds.dcs, nil
}

func (mds *MockDataStore) AddDc(dc Dc) error {
	mds.dcs = append(mds.dcs, dc)
	return nil
}

func (mds *MockDataStore) UpdateDc(id, newName string) error {
	return errors.New("NYI")
}

func (mds *MockDataStore) CloseSession() {}
