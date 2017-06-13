package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ContextLogic/eventmaster/eventmaster"
	statsd "github.com/ContextLogic/gobrubeckclient/brubeck"
	metrics "github.com/rcrowley/go-metrics"
	context "golang.org/x/net/context"
)

func NewGRPCServer(config *Config, s *EventStore, r metrics.Registry) (*grpcServer, error) {
	statsClient := statsd.NewClient(
		fmt.Sprintf("eventmaster_", config.EventStoreName),
		config.StatsdServer,
		false, // TODO disable this outside of prod
	)

	return &grpcServer{
		config:   config,
		statsd:   statsClient,
		store:    s,
		registry: r,
	}, nil
}

type grpcServer struct {
	config   *Config
	statsd   *statsd.Client
	store    *EventStore
	registry metrics.Registry
}

func (s *grpcServer) performOperation(method string, op func() (string, error)) (*eventmaster.WriteResponse, error) {
	name := "grpc" + method

	start := time.Now()
	timer := metrics.GetOrRegisterTimer(fmt.Sprintf("%s:%s", name, "Timer"), s.registry)
	defer timer.UpdateSince(start)
	meter := metrics.GetOrRegisterMeter(fmt.Sprintf("%s:%s", name, "Meter"), s.registry)
	meter.Mark(1)

	id, err := op()
	if err != nil {
		meter := metrics.GetOrRegisterMeter(name+"Error", s.registry)
		meter.Mark(1)
		fmt.Println("Error performing operation", method, err)
		return &eventmaster.WriteResponse{
			Errcode: 1,
			Errmsg:  err.Error(),
		}, err
	}

	successMeter := metrics.GetOrRegisterMeter(name+"Success", s.registry)
	successMeter.Mark(1)
	return &eventmaster.WriteResponse{
		Id: id,
	}, nil
}

func (s *grpcServer) AddEvent(ctx context.Context, evt *eventmaster.Event) (*eventmaster.WriteResponse, error) {
	return s.performOperation("AddEvent", func() (string, error) {
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

func (s *grpcServer) GetEvents(q *eventmaster.Query, stream eventmaster.EventMaster_GetEventsServer) error {
	name := "grpcGetEvents"

	start := time.Now()
	timer := metrics.GetOrRegisterTimer(fmt.Sprintf("%s:%s", name, "Timer"), s.registry)
	defer timer.UpdateSince(start)
	meter := metrics.GetOrRegisterMeter(fmt.Sprintf("%s:%s", name, "Meter"), s.registry)
	meter.Mark(1)

	events, err := s.store.Find(q)
	if err != nil {
		errMeter := metrics.GetOrRegisterMeter(name+"Error", s.registry)
		errMeter.Mark(1)
		fmt.Println("Error performing event store find", err)
		return err
	}
	for _, ev := range events {
		d, err := json.Marshal(ev.Data)
		if err != nil {
			errMeter := metrics.GetOrRegisterMeter(name+"Error", s.registry)
			errMeter.Mark(1)
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
			errMeter := metrics.GetOrRegisterMeter(name+"Error", s.registry)
			errMeter.Mark(1)
			fmt.Println("Error streaming event to grpc client", err)
			return err
		}
	}
	successMeter := metrics.GetOrRegisterMeter(name+"Success", s.registry)
	successMeter.Mark(1)
	return nil
}

func (s *grpcServer) AddTopic(ctx context.Context, t *eventmaster.Topic) (*eventmaster.WriteResponse, error) {
	return s.performOperation("AddTopic", func() (string, error) {
		var schema map[string]interface{}
		err := json.Unmarshal(t.DataSchema, &schema)
		if err != nil {
			return "", err
		}
		return s.store.AddTopic(TopicData{
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
		return s.store.UpdateTopic(t.OldName, TopicData{
			Name:   t.NewName,
			Schema: schema,
		})
	})
}

func (s *grpcServer) DeleteTopic(ctx context.Context, t *eventmaster.DeleteTopicRequest) (*eventmaster.WriteResponse, error) {
	name := "grpcDeleteTopic"

	start := time.Now()
	timer := metrics.GetOrRegisterTimer(fmt.Sprintf("%s:%s", name, "Timer"), s.registry)
	defer timer.UpdateSince(start)
	meter := metrics.GetOrRegisterMeter(fmt.Sprintf("%s:%s", name, "Meter"), s.registry)
	meter.Mark(1)

	err := s.store.DeleteTopic(t)
	if err != nil {
		errMeter := metrics.GetOrRegisterMeter(name+"Error", s.registry)
		errMeter.Mark(1)
		fmt.Println("Error deleting topic: ", err)
		return &eventmaster.WriteResponse{
			Errcode: 1,
			Errmsg:  err.Error(),
		}, err
	}
	successMeter := metrics.GetOrRegisterMeter(name+"Success", s.registry)
	successMeter.Mark(1)
	return &eventmaster.WriteResponse{}, nil
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
