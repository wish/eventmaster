package main

import (
	"fmt"

	"github.com/ContextLogic/eventmaster/eventmaster"
	statsd "github.com/ContextLogic/gobrubeckclient/brubeck"
	context "golang.org/x/net/context"
)

func NewServer(config *Config, s *EventStore) (*server, error) {
	statsClient := statsd.NewClient(
		fmt.Sprintf("eventmaster_", config.EventStoreName),
		config.StatsdServer,
		false, // TODO disable this outside of prod
	)

	return &server{
		config: config,
		statsd: statsClient,
		store:  s,
	}, nil
}

type server struct {
	config *Config
	statsd *statsd.Client
	store  *EventStore
}

func (s *server) Track(ctx context.Context, ev *eventmaster.Event) (*eventmaster.WriteResponse, error) {
	err := s.store.AddEvent(ev)
	if err != nil {
		fmt.Println("Error writing event to cassandra:", err)
		return &eventmaster.WriteResponse{
			Errcode: 1,
			Errmsg:  err.Error(),
		}, err
	}
	return &eventmaster.WriteResponse{}, nil
}

func (s *server) GetEvents(q *eventmaster.Query, stream eventmaster.EventMaster_GetEventsServer) error {
	events, err := s.store.Find(q)
	if err != nil {
		return err
	}
	for _, event := range events {
		stream.Send(&eventmaster.Event{
			Timestamp: event.Timestamp,
			Dc: event.Dc,
			TopicName: event.TopicName,
			Tags: event.Tags,
			Host: event.Host,
			User: event.User,
			Data: event.Data,
		})
	}
	return nil;
}

func (s *server) Healthcheck(ctx context.Context, in *eventmaster.HealthcheckRequest) (*eventmaster.HealthcheckResponse, error) {
	return &eventmaster.HealthcheckResponse{"OK"}, nil
}
