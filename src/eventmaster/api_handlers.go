package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/ContextLogic/eventmaster/eventmaster"
	"github.com/pkg/errors"
)

type eventAPIHandler struct {
	store *EventStore
}

type topicAPIHandler struct {
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

	var evt eventmaster.Event
	err := decoder.Decode(&evt)

	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error decoding JSON event")
		return
	}
	id, err := eah.store.AddEvent(&evt)
	if err != nil {
		sendError(w, http.StatusInternalServerError, err, "Error writing event")
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(id))
}

func (eah *eventAPIHandler) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	var q eventmaster.Query

	// read from request body first - if there's an error, read from query params
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&q)
	if err != nil {
		query := r.URL.Query()
		q.Dc = query["dc"]
		q.Host = query["host"]
		q.TargetHost = query["target_host"]
		q.TagSet = query["tag"]
		q.TopicName = query["topic_name"]
		q.SortField = query["sort_field"]
		for _, elem := range query["sort_ascending"] {
			if strings.ToLower(elem) == "true" {
				q.SortAscending = append(q.SortAscending, true)
			} else if strings.ToLower(elem) == "false" {
				q.SortAscending = append(q.SortAscending, false)
			}
		}
		if len(q.SortField) != len(q.SortAscending) {
			sendError(w, http.StatusBadRequest, errors.New("sort_field and sort_ascending don't match"), "Error")
		}
		startTime := query.Get("start_time")
		if startTime != "" {
			q.StartTime, _ = strconv.ParseInt(startTime, 10, 64)
		}
		endTime := query.Get("end_time")
		if endTime != "" {
			q.EndTime, _ = strconv.ParseInt(endTime, 10, 64)
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

func (tah *topicAPIHandler) handlePostTopic(w http.ResponseWriter, r *http.Request) {
	topicName := r.URL.Query().Get(":name")

	schema, err := ioutil.ReadAll(r.Body)
	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error reading body of request")
	}
	ok := tah.store.ValidateSchema(schema)
	if !ok {
		sendError(w, http.StatusBadRequest, err, "Schema is not in valid JSON format")
	}
	id, err := tah.store.AddTopic(topicName, string(schema))
	if err != nil {
		sendError(w, http.StatusInternalServerError, err, "Error adding topic")
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(id))
}

func (eah *eventAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		eah.handlePostEvent(w, r)
	} else if r.Method == "GET" {
		eah.handleGetEvent(w, r)
	}
}

func (eah *topicAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		eah.handlePostTopic(w, r)
	}
}
