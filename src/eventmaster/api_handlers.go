package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/ContextLogic/eventmaster/eventmaster"
)

type eventAPIHandler struct {
	store *EventStore
}

type SearchResult struct {
	Results []*Event `json:"results"`
}

func sendError(w http.ResponseWriter, code int, err error, message string) {
	w.WriteHeader(code)
	w.Write([]byte(fmt.Sprintf("%s: %s", message, err.Error())))
	return
}

func (eah *eventAPIHandler) handlePostEvent(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var evt Event
	err := decoder.Decode(&evt)
	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error decoding JSON event")
		return
	}
	err = eah.store.AddEvent(&evt)
	if err != nil {
		sendError(w, http.StatusInternalServerError, err, "Error writing event")
	}
	w.WriteHeader(http.StatusOK)
	w.Write(evt.EventID)
}

func (eah *eventAPIHandler) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	var q eventmaster.Query

	if r.Method == "GET" {
		dc := r.URL.Query().Get("dc")
		if dc != "" {
			q.Dc = strings.Split(dc, ",")
		}
		host := r.URL.Query().Get("host")
		if host != "" {
			q.Host = strings.Split(host, ",")
		}
		targetHost := r.URL.Query().Get("target_host")
		if targetHost != "" {
			q.TargetHost = strings.Split(targetHost, ",")
		}
		tag := r.URL.Query().Get("tag")
		if tag != "" {
			q.TagSet = strings.Split(tag, ",")
		}
		topicName := r.URL.Query().Get("topic_name")
		if topicName != "" {
			q.TopicName = strings.Split(topicName, ",")
		}
		startTime := r.URL.Query().Get("start_time")
		if startTime != "" {
			q.StartTime, _ = strconv.ParseInt(startTime, 10, 64)
		}
		endTime := r.URL.Query().Get("end_time")
		if endTime != "" {
			q.EndTime, _ = strconv.ParseInt(endTime, 10, 64)
		}
		q.SortField = r.URL.Query().Get("sort_field")
		sortAscending := r.URL.Query().Get("sort_ascending")
		if strings.ToLower(sortAscending) == "true" {
			q.SortAscending = true
		}
	} else if r.Method == "POST" {
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&q)
		if err != nil {
			sendError(w, http.StatusBadRequest, err, "Error decoding JSON query")
			return
		}
	}

	results, err := eah.store.Find(&q)
	if err != nil {
		sendError(w, http.StatusInternalServerError, err, "Error executing query")
	}
	sr := SearchResult{
		Results: results,
	}
	jsonSr, err := json.Marshal(sr)
	if err != nil {
		sendError(w, http.StatusInternalServerError, err, "Error marshalling results into JSON")
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonSr)
}

func (eah *eventAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		eah.handlePostEvent(w, r)
	} else if r.Method == "GET" {
		eah.handleGetEvent(w, r)
	}
}
