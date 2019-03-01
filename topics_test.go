package eventmaster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wish/eventmaster/jh"
	"github.com/pkg/errors"
)

func TestTopicRoundtrip(t *testing.T) {
	mds := &mockDataStore{}
	store, err := GetTestEventStore(mds)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}

	ts := httptest.NewServer(NewServer(store, "", ""))

	name := "test"

	{
		topics, err := store.GetTopics()
		if err != nil {
			t.Fatalf("get topics: %v", err)
		}
		if got, want := len(topics), 0; got != want {
			t.Fatalf("number of topics: got %v, want %v", got, want)
		}
	}

	id, err := postTopic(ts.URL, name)
	if err != nil {
		t.Fatalf("post topic: %v", err)
	}

	t.Logf("created topic: %v", id)

	{
		topics, err := store.GetTopics()
		if err != nil {
			t.Fatalf("get topics: %v", err)
		}
		if got, want := len(topics), 1; got != want {
			t.Fatalf("number of topics: got %v, want %v", got, want)
		}
	}

	{
		topics, err := getTopics(ts.URL, name)
		if err != nil {
			t.Fatalf("get topics: %v", err)
		}

		if got, want := len(topics), 1; got != want {
			t.Fatalf("number of topics: got %v, want %v", got, want)
		}

		if got, want := topics[0].ID, id; got != want {
			t.Fatalf("topic id: got %v, want %v", got, want)
		}
	}

	newName := "bar"
	id2, err := updateTopic(ts.URL, name, newName)
	if err != nil {
		t.Fatalf("patch: %v", err)
	}

	if got, want := id2, id; got != want {
		t.Fatalf("id changed; got %v, want %v", got, want)
	}

	{
		topics, err := getTopics(ts.URL, name)
		if err != nil {
			t.Fatalf("get dc: %v", err)
		}

		if got, want := len(topics), 1; got != want {
			t.Fatalf("number of topics: got %v, want %v", got, want)
		}

		if got, want := topics[0].ID, id; got != want {
			t.Fatalf("dc id: got %v, want %v", got, want)
		}
	}

	n, err := delTopic(ts.URL, newName)
	if err != nil {
		t.Fatalf("unable to delete topic: %v", err)
	}
	if got, want := n, newName; got != want {
		t.Fatalf("id changed; got %v, want %v", got, want)
	}

	{
		topics, err := store.GetTopics()
		if err != nil {
			t.Fatalf("get topics: %v", err)
		}
		if got, want := len(topics), 0; got != want {
			t.Fatalf("number of topics: got %v, want %v", got, want)
		}
	}
}

func postTopic(url, name string) (string, error) {
	return topic(http.MethodPost, url, name, "")
}

func updateTopic(url, name, new string) (string, error) {
	return topic(http.MethodPut, url, new, name)
}

func topic(method, url, new, name string) (string, error) {
	buf := &bytes.Buffer{}
	topic := Topic{Name: new}
	if err := json.NewEncoder(buf).Encode(topic); err != nil {
		return "", errors.Wrap(err, "json encode")
	}

	url = url + "/v1/topic"
	if name != "" {
		url = fmt.Sprintf("%v/%v", url, name)
	}
	req, err := http.NewRequest(method, url, buf)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "post topic")
	}

	buf.Reset()
	io.Copy(buf, resp.Body)
	resp.Body.Close()

	var status int
	switch method {
	case http.MethodPost:
		status = http.StatusCreated
	case http.MethodPut:
		status = http.StatusOK
	default:
		return "", errors.New("bad method")
	}

	if got, want := resp.StatusCode, status; got != want {
		return "", jh.NewError(errors.Errorf("bad status: got %v, want %v, %v", got, want, buf.String()).Error(), got)
	}

	res := struct {
		I string `json:"topic_id"`
	}{}
	if err := json.NewDecoder(buf).Decode(&res); err != nil {
		return "", errors.Wrap(err, "json decode")
	}
	return res.I, nil
}

func getTopics(url, name string) ([]Topic, error) {
	resp, err := http.Get(url + "/v1/topic")
	if err != nil {
		return nil, errors.Wrap(err, "get topic")
	}

	buf := &bytes.Buffer{}
	io.Copy(buf, resp.Body)
	resp.Body.Close()

	if got, want := resp.StatusCode, http.StatusOK; got != want {
		return nil, errors.Errorf("bad status: got %v, want %v, %v", got, want, buf.String())
	}

	r := map[string][]Topic{}
	if err := json.NewDecoder(buf).Decode(&r); err != nil {
		return nil, errors.Wrap(err, "json decode")
	}
	if _, ok := r["results"]; !ok {
		return nil, errors.New("missing result key from response")
	}
	return r["results"], nil
}

func delTopic(url, name string) (string, error) {
	url = fmt.Sprintf("%v/v1/topic/%v", url, name)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "delete topic")
	}

	buf := &bytes.Buffer{}
	io.Copy(buf, resp.Body)
	resp.Body.Close()

	if got, want := resp.StatusCode, http.StatusOK; got != want {
		return "", jh.NewError(errors.Errorf("bad status: got %v, want %v, %v", got, want, buf.String()).Error(), got)
	}

	r := map[string]string{}
	if err := json.NewDecoder(buf).Decode(&r); err != nil {
		return "", errors.Wrap(err, "json decode")
	}
	if _, ok := r["topic"]; !ok {
		return "", errors.New("missing result key from response")
	}
	return r["topic"], nil
}

func TestBadAddTopic(t *testing.T) {
	mds := &mockDataStore{}
	store, err := GetTestEventStore(mds)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}

	ts := httptest.NewServer(NewServer(store, "", ""))

	name := ""

	_, err = postTopic(ts.URL, name)
	if err == nil {
		t.Fatalf("should fail to post topic with empty name: %v", err)
	}
	if got, want := err.(jh.Error).Status(), http.StatusBadRequest; got != want {
		t.Fatalf("bad status: got %v, want %v", got, want)
	}
}

func TestBadDeleteTopic(t *testing.T) {
	mds := &mockDataStore{}
	store, err := GetTestEventStore(mds)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}

	ts := httptest.NewServer(NewServer(store, "", ""))

	name := "not-found"

	_, err = delTopic(ts.URL, name)
	if err == nil {
		t.Fatalf("should fail to post topic with empty name: %v", err)
	}
	if got, want := err.(jh.Error).Status(), http.StatusNotFound; got != want {
		t.Fatalf("bad status: got %v, want %v", got, want)
	}
}
