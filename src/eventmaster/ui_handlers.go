package main

import (
	"fmt"
	"html/template"
	"net/http"
)

type mainPageHandler struct {
	store *EventStore
}

type createPageHandler struct {
	store *EventStore
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
	Events     []*Event
}

func (mph *mainPageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t, err := template.New("main.html").ParseFiles("ui/templates/main.html", "ui/templates/query_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}

	err = t.Execute(w, pageData{
		Topics: mph.store.GetTopics(),
		Dcs:    mph.store.GetDcs(),
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("error executing template: %v", err), http.StatusInternalServerError)
	}
}

func (cph *createPageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t, err := template.New("main.html").ParseFiles("ui/templates/main.html", "ui/templates/create_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}

	err = t.Execute(w, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("error executing template: %v", err), http.StatusInternalServerError)
	}
}
