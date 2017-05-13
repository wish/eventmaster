package main

import (
	"fmt"
	"html/template"
	"net/http"
)

type mainPageHandler struct {
	store *EventStore
	fm    template.FuncMap
}

type getEventHandler struct {
	store *EventStore
	fm    template.FuncMap
}

type pageData struct {
	CurPage    int
	TotalPages int
	Dc         string
	Topic      string
	Host       string
	Date       string
	Dcs        []string
	Topics     []string
	Events     []*FullEvent
}

func (mph *mainPageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t, err := template.New("main.html").Funcs(mph.fm).ParseFiles("templates/main.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	err = t.Execute(w, pageData{})
	if err != nil {
		http.Error(w, fmt.Sprintf("error executing template: %v", err), http.StatusInternalServerError)
	}
}

func (geh *getEventHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing form: %v", err), http.StatusInternalServerError)
		return
	}
	filter := r.Form["filter"][0]
	text := r.Form["searchText"][0]
	dc := r.Form["dc"][0]
	var events []*FullEvent
	if filter == "topic" {
		events, err = geh.store.FindByTopic(text, dc, 1)
	} else if filter == "date" {
		events, err = geh.store.FindByDate(text, dc, 1)
	} else if filter == "host" {
		events, err = geh.store.FindByHost(text, dc, 1)
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("error finding results in cassandra: %v", err), http.StatusInternalServerError)
		return
	}

	t, err := template.New("main.html").Funcs(geh.fm).ParseFiles("templates/main.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	err = t.Execute(w, pageData{
		Events: events,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("error executing template: %v", err), http.StatusInternalServerError)
	}
}
