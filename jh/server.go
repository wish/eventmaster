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
	r := httprouter.New()
	s := &server{r}

	r.GET("/ok/", Adapter(s.ok))
	r.GET("/conflict/", Adapter(s.conflict))
	r.GET("/created/", Adapter(s.created))
	r.GET("/ise/", Adapter(s.ise))

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
