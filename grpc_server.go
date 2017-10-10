package eventmaster

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	context "golang.org/x/net/context"

	"github.com/ContextLogic/eventmaster/metrics"
	eventmaster "github.com/ContextLogic/eventmaster/proto"
)

// NewGRPCServer returns a populated
func NewGRPCServer(config *Flags, s *EventStore) *GRPCServer {
	return &GRPCServer{
		config: config,
		store:  s,
	}
}

// GRPCServer implements gRPC endpoints.
type GRPCServer struct {
	config *Flags
	store  *EventStore
}

func (s *GRPCServer) performOperation(method string, op func() (string, error)) (*eventmaster.WriteResponse, error) {
	start := time.Now()
	defer func() {
		metrics.GRPCLatency(method, start)
	}()

	id, err := op()
	if err != nil {
		metrics.GRPCFailure(method)
		return nil, errors.Wrapf(err, "operation %v", method)
	}

	metrics.GRPCSuccess(method)
	return &eventmaster.WriteResponse{
		ID: id,
	}, nil
}

// AddEvent adds an event to the datastore.
func (s *GRPCServer) AddEvent(ctx context.Context, evt *eventmaster.Event) (*eventmaster.WriteResponse, error) {
	return s.performOperation("AddEvent", func() (string, error) {
		if evt.Data == nil {
			evt.Data = []byte("{}")
		}
		var data map[string]interface{}
		err := json.Unmarshal(evt.Data, &data)
		if err != nil {
			return "", errors.Wrap(err, "json decode of data")
		}
		return s.store.AddEvent(&UnaddedEvent{
			ParentEventID: evt.ParentEventID,
			EventTime:     evt.EventTime,
			DC:            evt.DC,
			TopicName:     evt.TopicName,
			Tags:          evt.TagSet,
			Host:          evt.Host,
			TargetHosts:   evt.TargetHostSet,
			User:          evt.User,
			Data:          data,
		})
	})
}

// GetEventByID returns an event by id.
func (s *GRPCServer) GetEventByID(ctx context.Context, id *eventmaster.EventID) (*eventmaster.Event, error) {
	name := "GetEventByID"
	start := time.Now()
	defer func() {
		metrics.GRPCLatency(name, start)
	}()

	ev, err := s.store.FindByID(id.EventID)
	if err != nil {
		metrics.GRPCFailure(name)
		return nil, errors.Wrapf(err, "could not find by id", id.EventID)
	}
	d, err := json.Marshal(ev.Data)
	if err != nil {
		metrics.GRPCFailure(name)
		return nil, errors.Wrap(err, "data json marshal")
	}
	return &eventmaster.Event{
		EventID:       ev.EventID,
		ParentEventID: ev.ParentEventID,
		EventTime:     ev.EventTime,
		DC:            s.store.getDCName(ev.DCID),
		TopicName:     s.store.getTopicName(ev.TopicID),
		TagSet:        ev.Tags,
		Host:          ev.Host,
		TargetHostSet: ev.TargetHosts,
		User:          ev.User,
		Data:          d,
	}, nil
}

// GetEvents returns all Events.
func (s *GRPCServer) GetEvents(q *eventmaster.Query, stream eventmaster.EventMaster_GetEventsServer) error {
	name := "GetEvents"
	start := time.Now()
	defer func() {
		metrics.GRPCLatency(name, start)
	}()

	events, err := s.store.Find(q)
	if err != nil {
		metrics.GRPCFailure(name)
		return errors.Wrapf(err, "unable to find %v", q)
	}
	for _, ev := range events {
		d, err := json.Marshal(ev.Data)
		if err != nil {
			metrics.GRPCFailure(name)
			return errors.Wrap(err, "json marshal of data")
		}
		if err := stream.Send(&eventmaster.Event{
			EventID:       ev.EventID,
			ParentEventID: ev.ParentEventID,
			EventTime:     ev.EventTime,
			DC:            s.store.getDCName(ev.DCID),
			TopicName:     s.store.getTopicName(ev.TopicID),
			TagSet:        ev.Tags,
			Host:          ev.Host,
			TargetHostSet: ev.TargetHosts,
			User:          ev.User,
			Data:          d,
		}); err != nil {
			metrics.GRPCFailure(name)
			return errors.Wrap(err, "stream send")
		}
	}
	metrics.GRPCSuccess(name)
	return nil
}

// GetEventIDs returns all event ids.
func (s *GRPCServer) GetEventIDs(q *eventmaster.TimeQuery, stream eventmaster.EventMaster_GetEventIDsServer) error {
	name := "GetEventByIDs"
	start := time.Now()
	defer func() {
		metrics.GRPCLatency(name, start)
	}()

	streamProxy := func(eventID string) error {
		return stream.Send(&eventmaster.EventID{EventID: eventID})
	}
	return s.store.FindIDs(q, streamProxy)
}

// AddTopic is the gRPC verison of AddTopic.
func (s *GRPCServer) AddTopic(ctx context.Context, t *eventmaster.Topic) (*eventmaster.WriteResponse, error) {
	return s.performOperation("AddTopic", func() (string, error) {
		if t.DataSchema == nil {
			t.DataSchema = []byte("{}")
		}
		var schema map[string]interface{}
		err := json.Unmarshal(t.DataSchema, &schema)
		if err != nil {
			return "", errors.Wrap(err, "json unmarshal of data schema")
		}
		return s.store.AddTopic(Topic{
			Name:   t.TopicName,
			Schema: schema,
		})
	})
}

// UpdateTopic is the gRPC version of updating a topic.
func (s *GRPCServer) UpdateTopic(ctx context.Context, t *eventmaster.UpdateTopicRequest) (*eventmaster.WriteResponse, error) {
	return s.performOperation("UpdateTopic", func() (string, error) {
		var schema map[string]interface{}
		err := json.Unmarshal(t.DataSchema, &schema)
		if err != nil {
			return "", errors.Wrap(err, "json unmarshal of data schema")
		}
		return s.store.UpdateTopic(t.OldName, Topic{
			Name:   t.NewName,
			Schema: schema,
		})
	})
}

// DeleteTopic is the gRPC version of DeleteTopic.
func (s *GRPCServer) DeleteTopic(ctx context.Context, t *eventmaster.DeleteTopicRequest) (*eventmaster.WriteResponse, error) {
	name := "DeleteTopic"
	start := time.Now()
	defer func() {
		metrics.GRPCLatency(name, start)
	}()

	err := s.store.DeleteTopic(t)
	if err != nil {
		metrics.GRPCFailure(name)
		return nil, errors.Wrap(err, "delete topic")
	}
	metrics.GRPCSuccess(name)
	return &eventmaster.WriteResponse{}, nil
}

// GetTopics is the gRPC call that returns all topics.
func (s *GRPCServer) GetTopics(ctx context.Context, _ *eventmaster.EmptyRequest) (*eventmaster.TopicResult, error) {
	name := "GetTopics"
	start := time.Now()
	defer func() {
		metrics.GRPCLatency(name, start)
	}()

	topics, err := s.store.GetTopics()
	if err != nil {
		metrics.GRPCFailure(name)
		return nil, errors.Wrap(err, "get topics")
	}

	var topicResults []*eventmaster.Topic

	for _, topic := range topics {
		var schemaBytes []byte
		if topic.Schema == nil {
			schemaBytes = []byte("{}")
		} else {
			schemaBytes, err = json.Marshal(topic.Schema)
			if err != nil {
				metrics.GRPCFailure(name)
				return nil, errors.Wrap(err, "json marshal of schema")
			}
		}
		topicResults = append(topicResults, &eventmaster.Topic{
			ID:         topic.ID,
			TopicName:  topic.Name,
			DataSchema: schemaBytes,
		})
	}
	metrics.GRPCSuccess(name)
	return &eventmaster.TopicResult{
		Results: topicResults,
	}, nil
}

// AddDC is the gRPC version of adding a datacenter.
func (s *GRPCServer) AddDC(ctx context.Context, d *eventmaster.DC) (*eventmaster.WriteResponse, error) {
	return s.performOperation("AddDC", func() (string, error) {
		return s.store.AddDC(d)
	})
}

// UpdateDC is the gRPC version of updating a datacenter.
func (s *GRPCServer) UpdateDC(ctx context.Context, t *eventmaster.UpdateDCRequest) (*eventmaster.WriteResponse, error) {
	return s.performOperation("UpdateDC", func() (string, error) {
		return s.store.UpdateDC(t)
	})
}

// GetDCs is the gRPC version of getting all datacenters.
func (s *GRPCServer) GetDCs(ctx context.Context, _ *eventmaster.EmptyRequest) (*eventmaster.DCResult, error) {
	name := "GetDCs"
	start := time.Now()
	defer func() {
		metrics.GRPCLatency(name, start)
	}()

	dcs, err := s.store.GetDCs()
	if err != nil {
		metrics.GRPCFailure(name)
		return nil, errors.Wrap(err, "get dcs")
	}

	var dcResults []*eventmaster.DC

	for _, dc := range dcs {
		dcResults = append(dcResults, &eventmaster.DC{
			ID:     dc.ID,
			DCName: dc.Name,
		})
	}
	metrics.GRPCSuccess(name)
	return &eventmaster.DCResult{
		Results: dcResults,
	}, nil
}

// Healthcheck is the gRPC health endpoint.
func (s *GRPCServer) Healthcheck(ctx context.Context, in *eventmaster.HealthcheckRequest) (*eventmaster.HealthcheckResponse, error) {
	return &eventmaster.HealthcheckResponse{Response: "OK"}, nil
}
