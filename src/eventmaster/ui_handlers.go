package main

import (
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"

	"github.com/ContextLogic/eventmaster/eventmaster"
)

type mainPageHandler struct {
	store *EventStore
}

type getEventHandler struct {
	store *EventStore
	fm    template.FuncMap
}

type createEventHandler struct {
	store *EventStore
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

func (geh *getEventHandler) buildQuery(r *http.Request) (*eventmaster.Query, error) {
	err := r.ParseForm()
	if err != nil {
		return nil, err
	}

	var startingTime int64
	var endingTime int64

	if r.Form["startDate"][0] == "" {
		startingTime = -1
	} else {
		st, err := time.Parse("2006-01-02", r.Form["startDate"][0])
		if err != nil {
			return nil, err
		}
		startingTime = st.Unix()
	}

	if r.Form["endDate"][0] == "" {
		endingTime = -1
	} else {
		et, err := time.Parse("2006-01-02", r.Form["endDate"][0])
		if err != nil {
			return nil, err
		}
		endingTime = et.Unix()
	}

	return &eventmaster.Query{
		Dc:        strings.Split(r.Form["dc"][0], ","),
		Host:      strings.Split(r.Form["host"][0], ","),
		TopicName: strings.Split(r.Form["topic"][0], ","),
		StartTime: int64(startingTime),
		EndTime:   int64(endingTime),
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

	t, err := template.New("main.html").Funcs(geh.fm).ParseFiles("ui/templates/main.html", "ui/templates/query_form.html")
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

func (ceh *createEventHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing form: %v", err), http.StatusInternalServerError)
		return
	}
	// topic := r.Form["topic"][0]
	// dc := r.Form["dc"][0]
	// tags := r.Form["tags"][0]
	// host := r.Form["host"][0]
	// user := r.Form["user"][0]
	// data := r.Form["data"][0]
	// date := r.Form["date"][0]
	// timeOfDay := r.Form["time"][0]

	// TODO: use form validation so we don't have to return an error
	// if date == "" {
	// 	http.Error(w, fmt.Sprintf("date cannot be empty"), http.StatusBadRequest)
	// 	return
	// }
	// if timeOfDay == "" {
	// 	http.Error(w, fmt.Sprintf("time cannot be empty"), http.StatusBadRequest)
	// 	return
	// }
	// fullTime := date + " " + timeOfDay
	// ts, err := time.Parse("2006-01-02 15:04", fullTime)
	// if err != nil {
	// 	http.Error(w, fmt.Sprintf("invalid date entered: %v", err), http.StatusInternalServerError)
	// }

	// var tgs []string
	// if tags != "" {
	// 	tgs = strings.Split(tags, ",")
	// }

	_, err = ceh.store.AddEvent(&eventmaster.Event{})
	if err != nil {
		http.Error(w, fmt.Sprintf("error writing event to cassandra: %v", err), http.StatusInternalServerError)
	}

	http.Redirect(w, r, "/", 301)
}
