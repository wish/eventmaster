package jh

import (
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
)

// server exists to test this package.
type server struct {
	handler http.Handler
}

func (srv *server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	srv.handler.ServeHTTP(w, req)
}

func newServer() *server {
	sm := http.NewServeMux()
	s := &server{sm}

	sm.HandleFunc("/ok/", RouterToStd(Adapter(s.ok)))
	sm.HandleFunc("/conflict/", RouterToStd(Adapter(s.conflict)))
	sm.HandleFunc("/created/", RouterToStd(Adapter(s.created)))
	sm.HandleFunc("/ise/", RouterToStd(Adapter(s.ise)))

	return s
}

func (srv *server) ok(w http.ResponseWriter, r *http.Request, ps httprouter.Params) (interface{}, error) {
	return resp(), nil
}

func (srv *server) conflict(w http.ResponseWriter, r *http.Request, ps httprouter.Params) (interface{}, error) {
	if err := conflict(); err != nil {
		return nil, Wrap(err, "context")
	}

	// should not get here
	return resp(), nil
}

func (srv *server) created(w http.ResponseWriter, r *http.Request, ps httprouter.Params) (interface{}, error) {
	return NewSuccess(resp(), http.StatusCreated), nil
}

func (srv *server) ise(w http.ResponseWriter, r *http.Request, ps httprouter.Params) (interface{}, error) {
	return nil, errors.New("should get default code")
}

func conflict() error {
	return NewError("reason", http.StatusConflict)
}

func resp() map[string]int {
	return map[string]int{"a": 3, "b": 2}
}
