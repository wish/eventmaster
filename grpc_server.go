package eventmaster

import (
	"encoding/json"
	"fmt"
	"time"

	eventmaster "github.com/ContextLogic/eventmaster/proto"
	context "golang.org/x/net/context"
)

func NewGRPCServer(config *Config, s *EventStore) (*grpcServer, error) {
	return &grpcServer{
		config: config,
		store:  s,
	}, nil
}

type grpcServer struct {
	config *Config
	store  *EventStore
}

func (s *grpcServer) performOperation(method string, op func() (string, error)) (*eventmaster.WriteResponse, error) {
	start := time.Now()
	defer func() {
		grpcReqLatencies.WithLabelValues(method).Observe(trackTime(start))
	}()
	grpcReqCounter.WithLabelValues(method).Inc()

	id, err := op()
	if err != nil {
		grpcRespCounter.WithLabelValues(method, "1").Inc()
		fmt.Println("Error performing operation", method, err)
		return nil, err
	}

	grpcRespCounter.WithLabelValues(method, "0").Inc()
	return &eventmaster.WriteResponse{
		Id: id,
	}, nil
}

func (s *grpcServer) AddEvent(ctx context.Context, evt *eventmaster.Event) (*eventmaster.WriteResponse, error) {
	return s.performOperation("AddEvent", func() (string, error) {
		if evt.Data == nil {
			evt.Data = []byte("{}")
		}
		var data map[string]interface{}
		err := json.Unmarshal(evt.Data, &data)
		if err != nil {
			return "", err
		}
		return s.store.AddEvent(&UnaddedEvent{
			ParentEventID: evt.ParentEventId,
			EventTime:     evt.EventTime,
			Dc:            evt.Dc,
			TopicName:     evt.TopicName,
			Tags:          evt.TagSet,
			Host:          evt.Host,
			TargetHosts:   evt.TargetHostSet,
			User:          evt.User,
			Data:          data,
		})
	})
}

func (s *grpcServer) GetEventById(ctx context.Context, id *eventmaster.EventId) (*eventmaster.Event, error) {
	name := "GetEventById"
	start := time.Now()
	defer func() {
		grpcReqLatencies.WithLabelValues(name).Observe(trackTime(start))
	}()
	grpcReqCounter.WithLabelValues(name).Inc()

	ev, err := s.store.FindById(id.EventId)
	if err != nil {
		grpcRespCounter.WithLabelValues(name, "1").Inc()
		fmt.Println("Error performing event store find", err)
		return nil, err
	}
	d, err := json.Marshal(ev.Data)
	if err != nil {
		grpcRespCounter.WithLabelValues(name, "1").Inc()
		fmt.Println("Error marshalling event data into JSON", err)
		return nil, err
	}
	return &eventmaster.Event{
		EventId:       ev.EventID,
		ParentEventId: ev.ParentEventID,
		EventTime:     ev.EventTime,
		Dc:            s.store.getDcName(ev.DcID),
		TopicName:     s.store.getTopicName(ev.TopicID),
		TagSet:        ev.Tags,
		Host:          ev.Host,
		TargetHostSet: ev.TargetHosts,
		User:          ev.User,
		Data:          d,
	}, nil
}

func (s *grpcServer) GetEvents(q *eventmaster.Query, stream eventmaster.EventMaster_GetEventsServer) error {
	name := "GetEvents"
	start := time.Now()
	defer func() {
		grpcReqLatencies.WithLabelValues(name).Observe(trackTime(start))
	}()
	grpcReqCounter.WithLabelValues(name).Inc()

	events, err := s.store.Find(q)
	if err != nil {
		grpcRespCounter.WithLabelValues(name, "1").Inc()
		fmt.Println("Error performing event store find", err)
		return err
	}
	for _, ev := range events {
		d, err := json.Marshal(ev.Data)
		if err != nil {
			grpcRespCounter.WithLabelValues(name, "1").Inc()
			fmt.Println("Error marshalling event data into JSON", err)
			return err
		}
		if err := stream.Send(&eventmaster.Event{
			EventId:       ev.EventID,
			ParentEventId: ev.ParentEventID,
			EventTime:     ev.EventTime,
			Dc:            s.store.getDcName(ev.DcID),
			TopicName:     s.store.getTopicName(ev.TopicID),
			TagSet:        ev.Tags,
			Host:          ev.Host,
			TargetHostSet: ev.TargetHosts,
			User:          ev.User,
			Data:          d,
		}); err != nil {
			grpcRespCounter.WithLabelValues(name, "1").Inc()
			fmt.Println("Error streaming event to grpc client", err)
			return err
		}
	}
	grpcRespCounter.WithLabelValues(name, "0").Inc()
	return nil
}

func (s *grpcServer) GetEventIds(q *eventmaster.TimeQuery, stream eventmaster.EventMaster_GetEventIdsServer) error {
	name := "GetEventByIds"
	start := time.Now()
	defer func() {
		grpcReqLatencies.WithLabelValues(name).Observe(trackTime(start))
	}()

	streamProxy := func(eventId string) error {
		return stream.Send(&eventmaster.EventId{eventId})
	}
	return s.store.FindIds(q, streamProxy)
}

func (s *grpcServer) AddTopic(ctx context.Context, t *eventmaster.Topic) (*eventmaster.WriteResponse, error) {
	return s.performOperation("AddTopic", func() (string, error) {
		if t.DataSchema == nil {
			t.DataSchema = []byte("{}")
		}
		var schema map[string]interface{}
		err := json.Unmarshal(t.DataSchema, &schema)
		if err != nil {
			return "", err
		}
		return s.store.AddTopic(Topic{
			Name:   t.TopicName,
			Schema: schema,
		})
	})
}

func (s *grpcServer) UpdateTopic(ctx context.Context, t *eventmaster.UpdateTopicRequest) (*eventmaster.WriteResponse, error) {
	return s.performOperation("UpdateTopic", func() (string, error) {
		var schema map[string]interface{}
		err := json.Unmarshal(t.DataSchema, &schema)
		if err != nil {
			return "", err
		}
		return s.store.UpdateTopic(t.OldName, Topic{
			Name:   t.NewName,
			Schema: schema,
		})
	})
}

func (s *grpcServer) DeleteTopic(ctx context.Context, t *eventmaster.DeleteTopicRequest) (*eventmaster.WriteResponse, error) {
	name := "DeleteTopic"
	start := time.Now()
	defer func() {
		grpcReqLatencies.WithLabelValues(name).Observe(trackTime(start))
	}()
	grpcReqCounter.WithLabelValues(name).Inc()

	err := s.store.DeleteTopic(t)
	if err != nil {
		grpcRespCounter.WithLabelValues(name, "1").Inc()
		fmt.Println("Error deleting topic: ", err)
		return nil, err
	}
	grpcRespCounter.WithLabelValues(name, "0").Inc()
	return &eventmaster.WriteResponse{}, nil
}

func (s *grpcServer) GetTopics(ctx context.Context, _ *eventmaster.EmptyRequest) (*eventmaster.TopicResult, error) {
	name := "GetTopics"
	start := time.Now()
	defer func() {
		grpcReqLatencies.WithLabelValues(name).Observe(trackTime(start))
	}()
	grpcReqCounter.WithLabelValues(name).Inc()

	topics, err := s.store.GetTopics()
	if err != nil {
		grpcRespCounter.WithLabelValues(name, "1").Inc()
		fmt.Println("Error getting topics: ", err)
		return nil, err
	}

	var topicResults []*eventmaster.Topic

	for _, topic := range topics {
		var schemaBytes []byte
		if topic.Schema == nil {
			schemaBytes = []byte("{}")
		} else {
			schemaBytes, err = json.Marshal(topic.Schema)
			if err != nil {
				grpcRespCounter.WithLabelValues(name, "1").Inc()
				fmt.Println("Error marshalling topic schema: ", err)
				return nil, err
			}
		}
		topicResults = append(topicResults, &eventmaster.Topic{
			Id:         topic.ID,
			TopicName:  topic.Name,
			DataSchema: schemaBytes,
		})
	}
	grpcRespCounter.WithLabelValues(name, "0").Inc()
	return &eventmaster.TopicResult{
		Results: topicResults,
	}, nil
}

func (s *grpcServer) AddDc(ctx context.Context, d *eventmaster.Dc) (*eventmaster.WriteResponse, error) {
	return s.performOperation("AddDc", func() (string, error) {
		return s.store.AddDc(d)
	})
}

func (s *grpcServer) UpdateDc(ctx context.Context, t *eventmaster.UpdateDcRequest) (*eventmaster.WriteResponse, error) {
	return s.performOperation("UpdateDc", func() (string, error) {
		return s.store.UpdateDc(t)
	})
}

func (s *grpcServer) GetDcs(ctx context.Context, _ *eventmaster.EmptyRequest) (*eventmaster.DcResult, error) {
	name := "GetDcs"
	start := time.Now()
	defer func() {
		grpcReqLatencies.WithLabelValues(name).Observe(trackTime(start))
	}()
	grpcReqCounter.WithLabelValues(name).Inc()

	dcs, err := s.store.GetDcs()
	if err != nil {
		grpcRespCounter.WithLabelValues(name, "1").Inc()
		fmt.Println("Error getting topics: ", err)
		return nil, err
	}

	var dcResults []*eventmaster.Dc

	for _, dc := range dcs {
		dcResults = append(dcResults, &eventmaster.Dc{
			Id:     dc.ID,
			DcName: dc.Name,
		})
	}
	grpcRespCounter.WithLabelValues(name, "0").Inc()
	return &eventmaster.DcResult{
		Results: dcResults,
	}, nil
}

func (s *grpcServer) Healthcheck(ctx context.Context, in *eventmaster.HealthcheckRequest) (*eventmaster.HealthcheckResponse, error) {
	return &eventmaster.HealthcheckResponse{"OK"}, nil
}
