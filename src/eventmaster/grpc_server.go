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

func (s *grpcServer) FireEvent(ctx context.Context, evt *eventmaster.Event) (*eventmaster.WriteResponse, error) {
	meter := metrics.GetOrRegisterMeter("grpcFireEvent:Meter", s.registry)
	meter.Mark(1)
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("grpcFireEvent:Timer", s.registry)
	defer timer.UpdateSince(start)

	id, err := s.store.AddEvent(evt)
	if err != nil {
		fmt.Println("Error writing event to cassandra:", err)
		return &eventmaster.WriteResponse{
			Errcode: 1,
			Errmsg:  err.Error(),
		}, err
	}
	return &eventmaster.WriteResponse{
		EventId: id,
	}, nil
}

func (s *grpcServer) GetEvents(q *eventmaster.Query, stream eventmaster.EventMaster_GetEventsServer) error {
	meter := metrics.GetOrRegisterMeter("grpcGetEvents:Meter", s.registry)
	meter.Mark(1)
	start := time.Now()
	timer := metrics.GetOrRegisterTimer("grpcGetEvents:Timer", s.registry)
	defer timer.UpdateSince(start)

	events, err := s.store.Find(q)
	if err != nil {
		return err
	}
	for _, ev := range events {
		d, err := json.Marshal(ev.Data)
		if err != nil {
			return err
		}
		stream.Send(&eventmaster.Event{
			ParentEventId: ev.ParentEventID,
			EventTime:     ev.EventTime,
			Dc:            ev.Dc,
			TopicName:     ev.TopicName,
			TagSet:        ev.Tags,
			Host:          ev.Host,
			TargetHostSet: ev.TargetHosts,
			User:          ev.User,
			Data:          string(d),
		})
	}
	return nil
}
