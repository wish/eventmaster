package eventmaster

import (
	"encoding/json"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"

	"github.com/ContextLogic/eventmaster/jh"
	eventmaster "github.com/ContextLogic/eventmaster/proto"
)

func (s *Server) addTopic(w http.ResponseWriter, r *http.Request, _ httprouter.Params) (interface{}, error) {
	td := Topic{}
	if err := json.NewDecoder(r.Body).Decode(&td); err != nil {
		return td, jh.NewError(errors.Wrap(err, "json decode").Error(), http.StatusBadRequest)
	}

	if td.Name == "" {
		return td, jh.NewError(errors.New("Must include topic_name in request").Error(), http.StatusBadRequest)
	}

	id, err := s.store.AddTopic(td)
	if err != nil {
		return nil, jh.Wrap(err, "add topic")
	}
	return jh.NewSuccess(map[string]string{"topic_id": id}, http.StatusCreated), nil
}

func (s *Server) getTopic(w http.ResponseWriter, r *http.Request, _ httprouter.Params) (interface{}, error) {
	topics, err := s.store.GetTopics()
	if err != nil {
		return nil, jh.Wrap(err, "get topics")
	}

	ret := map[string][]Topic{"results": topics}
	return ret, nil
}

func (s *Server) updateTopic(w http.ResponseWriter, r *http.Request, ps httprouter.Params) (interface{}, error) {
	var td Topic
	if err := json.NewDecoder(r.Body).Decode(&td); err != nil {
		return td, jh.NewError(errors.Wrap(err, "json decode").Error(), http.StatusBadRequest)
	}

	topicName := ps.ByName("name")
	if topicName == "" {
		return nil, jh.NewError(errors.New("Must include topic name in request").Error(), http.StatusBadRequest)
	}

	id, err := s.store.UpdateTopic(topicName, td)
	if err != nil {
		return nil, jh.Wrap(err, "update topic")
	}
	return map[string]string{"topic_id": id}, nil
}

func (s *Server) deleteTopic(w http.ResponseWriter, r *http.Request, ps httprouter.Params) (interface{}, error) {
	name := ps.ByName("name")
	if name == "" {
		return nil, jh.NewError(errors.New("Must include topic name in request").Error(), http.StatusBadRequest)
	}

	req := &eventmaster.DeleteTopicRequest{
		TopicName: name,
	}
	if err := s.store.DeleteTopic(req); err != nil {
		return nil, jh.Wrap(err, "delete topic")
	}

	return map[string]string{"topic": name}, nil
}
