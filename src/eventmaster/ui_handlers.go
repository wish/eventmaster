package main

import (
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ContextLogic/eventmaster/eventmaster"
)

type mainPageHandler struct {
	store *EventStore
	fm    template.FuncMap
}

type getEventHandler struct {
	store *EventStore
	fm    template.FuncMap
}

type createEventHandler struct {
	store *EventStore
	fm    template.FuncMap
}

type createPageHandler struct {
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
	t, err := template.New("main.html").Funcs(mph.fm).ParseFiles("templates/main.html", "templates/query_form.html")
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

func (geh *getEventHandler) buildQuery(r *http.Request) (*Query, error) {
	err := r.ParseForm()
	if err != nil {
		return nil, err
	}

	var startingTime int
	var endingTime int

	if r.Form["startingTime"][0] == "" {
		startingTime = -1
	} else {
		startingTime, err = strconv.Atoi(r.Form["startingTime"][0])
		if err != nil {
			return nil, err
		}
	}

	if r.Form["endingTime"][0] == "" {
		endingTime = -1
	} else {
		endingTime, err = strconv.Atoi(r.Form["endingTime"][0])
		if err != nil {
			return nil, err
		}
	}

	return &Query{
		Dc:        r.Form["dc"][0],
		Host:      r.Form["host"][0],
		TopicName: r.Form["topic"][0],
		TimeStart: int64(startingTime),
		TimeEnd:   int64(endingTime),
	}, nil
}

func (geh *getEventHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q, err := geh.buildQuery(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("couldn't build query from form: %v", err), http.StatusInternalServerError)
		return
	}

	events, err := geh.store.Find(q)
	if err != nil {
		http.Error(w, fmt.Sprintf("error finding results in cassandra: %v", err), http.StatusInternalServerError)
		return
	}

	t, err := template.New("main.html").Funcs(geh.fm).ParseFiles("templates/main.html", "templates/query_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}

	err = t.Execute(w, pageData{
		Events: events,
		Topics: geh.store.GetTopics(),
		Dcs:    geh.store.GetDcs(),
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("error executing template: %v", err), http.StatusInternalServerError)
	}
}

func (cph *createPageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t, err := template.New("main.html").Funcs(cph.fm).ParseFiles("templates/main.html", "templates/create_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}

	err = t.Execute(w, pageData{
		Topics: cph.store.GetTopics(),
		Dcs:    cph.store.GetDcs(),
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("error executing template: %v", err), http.StatusInternalServerError)
	}
}

func (ceh *createEventHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing form: %v", err), http.StatusInternalServerError)
		return
	}
	topic := r.Form["topic"][0]
	dc := r.Form["dc"][0]
	tags := r.Form["tags"][0]
	host := r.Form["host"][0]
	user := r.Form["user"][0]
	data := r.Form["data"][0]
	date := r.Form["date"][0]
	timeOfDay := r.Form["time"][0]

	// TODO: use form validation so we don't have to return an error
	if date == "" {
		http.Error(w, fmt.Sprintf("date cannot be empty"), http.StatusInternalServerError)
		return
	}
	if timeOfDay == "" {
		http.Error(w, fmt.Sprintf("date cannot be empty"), http.StatusInternalServerError)
		return
	}
	fullTime := date + " " + timeOfDay
	ts, err := time.Parse("2006-01-02 15:04", fullTime)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid date entered: %v", err), http.StatusInternalServerError)
	}

	err = ceh.store.AddEvent(&eventmaster.Event{
		Timestamp: ts.Unix(),
		Dc:        dc,
		TopicName: topic,
		Tags:      strings.Split(tags, ","),
		Host:      host,
		User:      user,
		Data:      data,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("error writing event to cassandra: %v", err), http.StatusInternalServerError)
	}

	http.Redirect(w, r, "/", 301)
}
