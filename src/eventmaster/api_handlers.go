package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type addEventAPIHandler struct {
	store *EventStore
}
type bulkEventAPIHandler struct {
	store *EventStore
}
type getEventAPIHandler struct {
	store *EventStore
}

func (aeh *addEventAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var evt Event
	err := decoder.Decode(&evt)
	if err != nil {
		fmt.Println("Error decoding event", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("Error decoding JSON event: %s", err)))
		return
	}
	aeh.store.AddEvent(&evt)
	w.WriteHeader(http.StatusOK)
}

func (beh *bulkEventAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

}

func (geh *getEventAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
}
