package main

import (
	"fmt"
	"html/template"
	"net/http"
)

type mainPageHandler struct {
	store *EventStore
}

type getEventHandler struct {
	store *EventStore
}

func (mph *mainPageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var events []*FullEvent
	t, err := template.New("main.html").ParseFiles("templates/main.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	err = t.Execute(w, events)
	if err != nil {
		http.Error(w, fmt.Sprintf("error executing template: %v", err), http.StatusInternalServerError)
	}
}

func (geh *getEventHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing form: %v", err), http.StatusInternalServerError)
	}
	filter := r.Form["filter"][0]
	text := r.Form["searchText"][0]
	dc := r.Form["dc"][0]
	var events []*FullEvent
	if filter == "topic" {
		events, err = geh.store.FindByTopic(text, dc)
	} else if filter == "date" {
		events, err = geh.store.FindByDate(text, dc)
	} else if filter == "host" {
		events, err = geh.store.FindByHost(text, dc)
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("error finding results in cassandra: %v", err), http.StatusInternalServerError)
		return
	}

	t, err := template.New("main.html").ParseFiles("templates/main.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	err = t.Execute(w, events)
	if err != nil {
		http.Error(w, fmt.Sprintf("error executing template: %v", err), http.StatusInternalServerError)
	}
}
