package eventmaster

import (
	"github.com/pkg/errors"

	proto "github.com/ContextLogic/eventmaster/proto"
)

type mockDataStore struct {
	events []*Event

	dcs    []Dc
	topics []Topic
}

func (mds *mockDataStore) AddEvent(e *Event) error {
	mds.events = append(mds.events, e)
	return nil
}

func (mds *MockDataStore) Find(q *proto.Query, topicIds []string, dcIds []string) (Events, error) {
	// for some reason we convert to ms randomly throughout the code
	q.StartEventTime *= 1000
	q.EndEventTime *= 1000

	ts := map[string]bool{}
	ds := map[string]bool{}
	for _, tid := range topicIds {
		ts[tid] = true
	}
	for _, dc := range dcIds {
		ds[dc] = true
	}

	r := Events{}
	for _, ev := range mds.events {
		if !(ev.EventTime > q.StartEventTime && ev.EventTime < q.EndEventTime) {
			continue
		}
		if topicIds != nil {
			if _, ok := ts[ev.TopicID]; !ok {
				continue
			}
		}
		if dcIds != nil {
			if _, ok := ds[ev.DcID]; !ok {
				continue
			}
		}
		r = append(r, ev)
	}
	return r, nil
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

func (mds *mockDataStore) CloseSession() {}
