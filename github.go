package eventmaster

import (
	"encoding/json"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"

	"github.com/wish/eventmaster/jh"
)

func (s *Server) gitHubEvent(w http.ResponseWriter, r *http.Request, _ httprouter.Params) (interface{}, error) {
	var info map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
		return nil, jh.NewError(errors.Wrap(err, "json decode").Error(), http.StatusBadRequest)
	}

	id, err := s.store.AddEvent(&UnaddedEvent{
		DC:        "github",
		Host:      "github",
		TopicName: "github",
		Data:      info,
	})
	if err != nil {
		return nil, jh.Wrap(err, "add event")
	}
	return map[string]string{"event_id": id}, nil
}
