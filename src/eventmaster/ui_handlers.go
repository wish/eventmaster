package main

import (
	"fmt"
	"html/template"
	"net/http"
)

func executeTemplate(w http.ResponseWriter, t *template.Template, data interface{}) {
	err := t.Execute(w, data)
	if err != nil {
		http.Error(w, fmt.Sprintf("error executing template: %v", err), http.StatusInternalServerError)
	}
}

type mainPageHandler struct {
	store *EventStore
}

func (mph *mainPageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t, err := template.New("main.html").ParseFiles("ui/templates/main.html", "ui/templates/query_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	executeTemplate(w, t, nil)
}

type createPageHandler struct {
	store *EventStore
}

func (cph *createPageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t, err := template.New("main.html").ParseFiles("ui/templates/main.html", "ui/templates/create_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	executeTemplate(w, t, nil)
}

type topicPageHandler struct {
	store *EventStore
}

func (tph *topicPageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t, err := template.New("main.html").ParseFiles("ui/templates/main.html", "ui/templates/topic_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	executeTemplate(w, t, nil)
}

type dcPageHandler struct {
	store *EventStore
}

func (tph *dcPageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t, err := template.New("main.html").ParseFiles("ui/templates/main.html", "ui/templates/dc_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	executeTemplate(w, t, nil)
}
