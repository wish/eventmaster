package eventmaster

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/pkg/errors"

	eventmaster "github.com/wish/eventmaster/proto"
)

func getQueryFromRequest(r *http.Request) (*eventmaster.Query, error) {
	var q eventmaster.Query

	// read from request body first - if there's an error, read from query params
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		query := r.URL.Query()
		q.ParentEventID = query["parent_event_id"]
		q.DC = query["dc"]
		q.Host = query["host"]
		q.TargetHostSet = query["target_host_set"]
		q.User = query["user"]
		q.TagSet = query["tag_set"]
		q.ExcludeTagSet = query["exclude_tag_set"]
		q.TopicName = query["topic_name"]
		if len(query["data"]) > 0 {
			q.Data = query["data"][0]
		}
		if startEventTime := query.Get("start_event_time"); startEventTime != "" {
			q.StartEventTime, err = strconv.ParseInt(startEventTime, 10, 64)
			if err != nil {
				return &q, errors.Wrap(err, "parse start event time")
			}
		}
		if endEventTime := query.Get("end_event_time"); endEventTime != "" {
			q.EndEventTime, err = strconv.ParseInt(endEventTime, 10, 64)
			if err != nil {
				return &q, errors.Wrap(err, "parse end event time")
			}
		}
		if startReceivedTime := query.Get("start_received_time"); startReceivedTime != "" {
			q.StartReceivedTime, err = strconv.ParseInt(startReceivedTime, 10, 64)
			if err != nil {
				return &q, errors.Wrap(err, "parse start received time")
			}
		}
		if endReceivedTime := query.Get("end_received_time"); endReceivedTime != "" {
			q.EndReceivedTime, err = strconv.ParseInt(endReceivedTime, 10, 64)
			if err != nil {
				return &q, errors.Wrap(err, "parse end received time")
			}
		}
		if start := query.Get("start"); start != "" {
			startIndex, _ := strconv.ParseInt(start, 10, 32)
			q.Start = int32(startIndex)
		}
		if limit := query.Get("limit"); limit != "" {
			resultSize, _ := strconv.ParseInt(limit, 10, 32)
			q.Limit = int32(resultSize)
		}
		if tagAndOperator := query.Get("tag_and_operator"); tagAndOperator == "true" {
			q.TagAndOperator = true
		}
		if targetHostAndOperator := query.Get("target_host_and_operator"); targetHostAndOperator == "true" {
			q.TargetHostAndOperator = true
		}
	}
	return &q, nil
}
