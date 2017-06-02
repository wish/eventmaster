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

func sendError(w http.ResponseWriter, code int, err error, message string) {
	w.WriteHeader(code)
	w.Write([]byte(fmt.Sprintf("%s: %s", message, err.Error())))
}

func sendResp(w http.ResponseWriter, key string, val string) {
	resp := make(map[string]string)
	resp[key] = val
	str, err := json.Marshal(resp)
	if err != nil {
		sendError(w, http.StatusInternalServerError, err, "Error marshalling response to JSON")
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Write(str)
}

type addEventAPIHandler struct {
	store *EventStore
}

func (eah *addEventAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)

	var evt eventmaster.Event
	err := decoder.Decode(&evt)

	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error decoding JSON event")
		return
	}
	id, err := eah.store.AddEvent(&evt)
	if err != nil {
		fmt.Println("Error adding event to store: ", err)
		sendError(w, http.StatusBadRequest, err, "Error writing event")
		return
	}
	sendResp(w, "event_id", id)
}

type getEventAPIHandler struct {
	store *EventStore
}

type SearchResult struct {
	Results []*Event `json:"results"`
}

func (eah *getEventAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
			return
		}
		if len(query["data"]) > 0 {
			q.Data = query["data"][0]
		}
		startEventTime := query.Get("start_event_time")
		if startEventTime != "" {
			q.StartEventTime, _ = strconv.ParseInt(startEventTime, 10, 64)
		}
		endEventTime := query.Get("end_event_time")
		if endEventTime != "" {
			q.EndEventTime, _ = strconv.ParseInt(endEventTime, 10, 64)
		}
		startReceivedTime := query.Get("start_received_time")
		if startReceivedTime != "" {
			q.StartReceivedTime, _ = strconv.ParseInt(startReceivedTime, 10, 64)
		}
		endReceivedTime := query.Get("end_received_time")
		if endReceivedTime != "" {
			q.EndReceivedTime, _ = strconv.ParseInt(endReceivedTime, 10, 64)
		}
	}

	results, err := eah.store.Find(&q)
	if err != nil {
		sendError(w, http.StatusInternalServerError, err, "Error executing query")
		return
	}
	sr := SearchResult{
		Results: results,
	}
	jsonSr, err := json.Marshal(sr)
	if err != nil {
		sendError(w, http.StatusInternalServerError, err, "Error marshalling results into JSON")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonSr)
}

type addTopicAPIHandler struct {
	store *EventStore
}

func (tah *addTopicAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var td TopicData
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error reading request body")
		return
	}
	err = json.Unmarshal(reqBody, &td)
	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error JSON decoding body of request")
		return
	}

	id, err := tah.store.AddTopic(td.Name, td.Schema)
	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error adding topic")
		return
	}
	sendResp(w, "topic_id", id)
}

type updateTopicAPIHandler struct {
	store *EventStore
}

func (tah *updateTopicAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var td TopicData
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error reading request body")
		return
	}
	err = json.Unmarshal(reqBody, &td)
	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error JSON decoding body of request")
		return
	}

	topicName := r.URL.Query().Get(":name")
	if topicName == "" {
		sendError(w, http.StatusBadRequest, err, "Error updating topic, no topic name provided")
		return
	}
	id, err := tah.store.UpdateTopic(topicName, td.Name, td.Schema)
	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error updating topic")
		return
	}
	sendResp(w, "topic_id", id)
}

type getTopicAPIHandler struct {
	store *EventStore
}

func (tah *getTopicAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	topicSet := make(map[string][]TopicData)
	topics, err := tah.store.GetTopics()
	if err != nil {
		sendError(w, http.StatusInternalServerError, err, "Error getting topics from store")
		return
	}
	topicSet["results"] = topics
	str, err := json.Marshal(topicSet)
	if err != nil {
		sendError(w, http.StatusInternalServerError, err, "Error marshalling response to JSON")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(str)
}

type addDcAPIHandler struct {
	store *EventStore
}

func (dah *addDcAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var dd DcData
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error reading request body")
		return
	}
	err = json.Unmarshal(reqBody, &dd)
	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error JSON decoding body of request")
		return
	}
	id, err := dah.store.AddDc(dd.Name)
	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error adding dc")
		return
	}
	sendResp(w, "dc_id", id)
}

type updateDcAPIHandler struct {
	store *EventStore
}

func (dah *updateDcAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var dd DcData
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error reading request body")
		return
	}
	err = json.Unmarshal(reqBody, &dd)
	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error JSON decoding body of request")
		return
	}
	dcName := r.URL.Query().Get(":name")
	if dcName == "" {
		sendError(w, http.StatusBadRequest, err, "Error updating topic, no topic name provided")
		return
	}
	id, err := dah.store.UpdateDc(dcName, dd.Name)
	if err != nil {
		sendError(w, http.StatusBadRequest, err, "Error updating dc")
		return
	}
	sendResp(w, "dc_id", id)
}

type getDcAPIHandler struct {
	store *EventStore
}

func (dah *getDcAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	dcSet := make(map[string][]string)
	dcs, err := dah.store.GetDcs()
	if err != nil {
		sendError(w, http.StatusInternalServerError, err, "Error getting dcs from store")
		return
	}
	dcSet["results"] = dcs
	str, err := json.Marshal(dcSet)
	if err != nil {
		sendError(w, http.StatusInternalServerError, err, "Error marshalling response to JSON")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(str)
}
