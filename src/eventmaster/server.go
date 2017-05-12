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

func (s *server) Healthcheck(ctx context.Context, in *eventmaster.HealthcheckRequest) (*eventmaster.HealthcheckResponse, error) {
	return &eventmaster.HealthcheckResponse{"OK"}, nil
}
