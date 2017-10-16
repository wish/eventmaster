package jh

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

// JSONHandler is the function interface used so that an Adapter can
// automatically parse out status codes and set headers.
//
// The first parameter is sent out as encoded json if error is nil, otherwise
// an error response is sent out as json.
type JSONHandler func(http.ResponseWriter, *http.Request, httprouter.Params) (interface{}, error)

// Adapter is middleware that converts a JSONHandler route into a normal
// http.HandlerFunc.
//
// It inspects the return against a collection of interfaces to determine
// status codes and what to encode out as json. It correctly sets status code,
// and content-type.
func Adapter(jh JSONHandler) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		r, err := jh(w, req, ps)
		if err != nil {
			status := http.StatusInternalServerError
			switch err := err.(type) {
			case Error:
				status = err.Status()
			}
			w.WriteHeader(status)

			r := map[string]string{"error": err.Error()}
			if err := json.NewEncoder(w).Encode(&r); err != nil {
				log.Printf("json encode: %v", err)
			}
			return
		}

		var d interface{}
		switch s := r.(type) {
		case Success:
			d = s.Data()
		default:
			d = r
		}

		status := http.StatusOK
		switch s := r.(type) {
		case HasStatus:
			status = s.Status()
		}
		w.WriteHeader(status)

		if err := json.NewEncoder(w).Encode(&d); err != nil {
			log.Printf("%v: json encode failure: %+v", req.URL.Path, err)
		}
	}
}
