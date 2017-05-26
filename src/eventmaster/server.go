package main

import (
	"encoding/json"
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

func (s *server) FireEvent(ctx context.Context, evt *eventmaster.Event) (*eventmaster.WriteResponse, error) {
	id, err := s.store.AddEvent(evt)
	if err != nil {
		fmt.Println("Error writing event to cassandra:", err)
		return &eventmaster.WriteResponse{
			Errcode: 1,
			Errmsg:  err.Error(),
			EventId: id,
		}, err
	}
	return &eventmaster.WriteResponse{}, nil
}

func (s *server) GetEvents(q *eventmaster.Query, stream eventmaster.EventMaster_GetEventsServer) error {
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
			EventType:     ev.EventType,
		})
	}
	return nil
}

func (s *server) Healthcheck(ctx context.Context, in *eventmaster.HealthcheckRequest) (*eventmaster.HealthcheckResponse, error) {
	return &eventmaster.HealthcheckResponse{"OK"}, nil
}
