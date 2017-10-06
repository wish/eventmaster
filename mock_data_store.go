package eventmaster

import (
	"github.com/pkg/errors"

	proto "github.com/ContextLogic/eventmaster/proto"
)

type mockDataStore struct {
	events []*Event

	dcs    []DC
	topics []Topic
}

func (mds *mockDataStore) AddEvent(e *Event) error {
	mds.events = append(mds.events, e)
	return nil
}

func (mds *mockDataStore) Find(q *proto.Query, topicIds []string, DCIDs []string) (Events, error) {
	// for some reason we convert to ms randomly throughout the code
	q.StartEventTime *= 1000
	q.EndEventTime *= 1000

	ts := map[string]bool{}
	ds := map[string]bool{}
	for _, tid := range topicIds {
		ts[tid] = true
	}
	for _, dc := range DCIDs {
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
		if DCIDs != nil {
			if _, ok := ds[ev.DCID]; !ok {
				continue
			}
		}
		r = append(r, ev)
	}
	return r, nil
}

func (mds *mockDataStore) FindByID(id string, data bool) (*Event, error) {
	return nil, errors.New("NYI")
}

func (mds *mockDataStore) FindIDs(*proto.TimeQuery, HandleEvent) error {
	return errors.New("NYI")
}

func (mds *mockDataStore) GetTopics() ([]Topic, error) {
	return mds.topics, nil
}

func (mds *mockDataStore) AddTopic(rt RawTopic) error {
	mds.topics = append(mds.topics, Topic{ID: rt.ID, Name: rt.Name})
	return nil
}

func (mds *mockDataStore) UpdateTopic(rt RawTopic) error {
	return errors.New("NYI")
}

func (mds *mockDataStore) DeleteTopic(string) error {
	return errors.New("NYI")
}

func (mds *mockDataStore) GetDCs() ([]DC, error) {
	return mds.dcs, nil
}

func (mds *mockDataStore) AddDC(dc DC) error {
	mds.dcs = append(mds.dcs, dc)
	return nil
}

func (mds *mockDataStore) UpdateDC(id, newName string) error {
	return errors.New("NYI")
}

func (mds *mockDataStore) CloseSession() {}
