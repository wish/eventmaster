package eventmaster

import (
	"encoding/json"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"

	"github.com/ContextLogic/eventmaster/jh"
	eventmaster "github.com/ContextLogic/eventmaster/proto"
)

func (s *Server) addDC(w http.ResponseWriter, r *http.Request, _ httprouter.Params) (interface{}, error) {
	var dd DC
	if err := json.NewDecoder(r.Body).Decode(&dd); err != nil {
		return dd, jh.NewError(errors.Wrap(err, "json decode").Error(), http.StatusBadRequest)
	}

	id, err := s.store.AddDC(&eventmaster.DC{
		DCName: dd.Name,
	})
	if err != nil {
		return nil, jh.Wrap(err, "add dc")
	}
	return jh.NewSuccess(map[string]string{"dc_id": id}, http.StatusCreated), nil
}

func (s *Server) getDC(w http.ResponseWriter, r *http.Request, _ httprouter.Params) (interface{}, error) {
	dcs, err := s.store.GetDCs()
	if err != nil {
		return dcs, jh.Wrap(err, "get dcs")
	}
	return map[string][]DC{"results": dcs}, nil
}

func (s *Server) updateDC(w http.ResponseWriter, r *http.Request, ps httprouter.Params) (interface{}, error) {
	var dd DC
	if err := json.NewDecoder(r.Body).Decode(&dd); err != nil {
		return dd, jh.NewError(errors.Wrap(err, "json decode").Error(), http.StatusBadRequest)
	}
	dcName := ps.ByName("name")
	if dcName == "" {
		// This *ought* to be ureachable code; if someone does a PUT without
		// the trailing name they'll get http.StatusMethodNotAllowed
		return nil, jh.NewError(errors.New("Must include dc name in request").Error(), http.StatusBadRequest)
	}

	id, err := s.store.UpdateDC(&eventmaster.UpdateDCRequest{
		OldName: dcName,
		NewName: dd.Name,
	})
	if err != nil {
		return nil, jh.Wrap(err, "update dc")
	}
	return map[string]string{"dc_id": id}, nil
}
