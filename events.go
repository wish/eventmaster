package eventmaster

import (
	"encoding/json"
	"net/http"

	"github.com/ContextLogic/eventmaster/jh"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
)

// EventResult is the json-serializable version of an Event.
type EventResult struct {
	EventID       string                 `json:"event_id"`
	ParentEventID string                 `json:"parent_event_id"`
	EventTime     int64                  `json:"event_time"`
	DC            string                 `json:"dc"`
	TopicName     string                 `json:"topic_name"`
	Tags          []string               `json:"tag_set"`
	Host          string                 `json:"host"`
	TargetHosts   []string               `json:"target_host_set"`
	User          string                 `json:"user"`
	Data          map[string]interface{} `json:"data"`
	ReceivedTime  int64                  `json:"received_time"`
}

// SearchResult groups a slice of EventResult for http responses.
type SearchResult struct {
	Results []*EventResult `json:"results"`
}

func (s *Server) addEvent(w http.ResponseWriter, r *http.Request, _ httprouter.Params) (interface{}, error) {
	var evt UnaddedEvent
	if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
		return evt, jh.NewError(errors.Wrap(err, "json decode").Error(), http.StatusBadRequest)
	}

	id, err := s.store.AddEvent(&evt)
	if err != nil {
		return nil, jh.Wrap(err, "add event")
	}
	return map[string]string{"event_id": id}, nil
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request, _ httprouter.Params) (interface{}, error) {
	q, err := getQueryFromRequest(r)
	if err != nil {
		return q, jh.NewError(errors.Wrap(err, "get query from request").Error(), http.StatusBadRequest)
	}

	events, err := s.store.Find(q)
	if err != nil {
		return events, errors.Wrap(err, "find events")
	}

	sr := SearchResult{}
	for _, ev := range events {
		sr.Results = append(sr.Results, &EventResult{
			EventID:       ev.EventID,
			ParentEventID: ev.ParentEventID,
			EventTime:     ev.EventTime,
			DC:            s.store.getDCName(ev.DCID),
			TopicName:     s.store.getTopicName(ev.TopicID),
			Tags:          ev.Tags,
			Host:          ev.Host,
			TargetHosts:   ev.TargetHosts,
			User:          ev.User,
			Data:          ev.Data,
		})
	}
	return sr, nil
}

func (s *Server) getEventByID(w http.ResponseWriter, r *http.Request, ps httprouter.Params) (interface{}, error) {
	eventID := ps.ByName("id")
	if eventID == "" {
		return nil, errors.New("did not provide event id")
	}

	ev, err := s.store.FindByID(eventID)
	if err != nil {
		return ev, errors.Wrap(err, "find by id")
	}

	ret := map[string]EventResult{
		"result": {
			EventID:       ev.EventID,
			ParentEventID: ev.ParentEventID,
			EventTime:     ev.EventTime,
			DC:            s.store.getDCName(ev.DCID),
			TopicName:     s.store.getTopicName(ev.TopicID),
			Tags:          ev.Tags,
			Host:          ev.Host,
			TargetHosts:   ev.TargetHosts,
			User:          ev.User,
			Data:          ev.Data,
		},
	}
	return ret, nil
}
