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

func TestDCRoundtrip(t *testing.T) {
	mds := &mockDataStore{}
	store, err := GetTestEventStore(mds)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}

	ts := httptest.NewServer(NewServer(store, "", ""))

	name := "test"

	{
		dcs, err := store.GetDCs()
		if err != nil {
			t.Fatalf("get dcs: %v", err)
		}
		if got, want := len(dcs), 0; got != want {
			t.Fatalf("number of dcs: got %v, want %v", got, want)
		}
	}

	id, err := postDC(ts.URL, name)
	if err != nil {
		t.Fatalf("post dc: %v", err)
	}

	t.Logf("created dc: %v", id)

	{
		dcs, err := store.GetDCs()
		if err != nil {
			t.Fatalf("get dcs: %v", err)
		}
		if got, want := len(dcs), 1; got != want {
			t.Fatalf("number of dcs: got %v, want %v", got, want)
		}
	}

	{
		dcs, err := getDCs(ts.URL, name)
		if err != nil {
			t.Fatalf("get dc: %v", err)
		}

		if got, want := len(dcs), 1; got != want {
			t.Fatalf("number of dcs: got %v, want %v", got, want)
		}

		if got, want := dcs[0].ID, id; got != want {
			t.Fatalf("dc id: got %v, want %v", got, want)
		}
	}

	newName := "bar"
	id2, err := updateDC(ts.URL, name, newName)
	if err != nil {
		t.Fatalf("patch: %v", err)
	}

	if got, want := id2, id; got != want {
		t.Fatalf("id changed; got %v, want %v", got, want)
	}

	{
		dcs, err := getDCs(ts.URL, name)
		if err != nil {
			t.Fatalf("get dc: %v", err)
		}

		if got, want := len(dcs), 1; got != want {
			t.Fatalf("number of dcs: got %v, want %v", got, want)
		}

		if got, want := dcs[0].ID, id; got != want {
			t.Fatalf("dc id: got %v, want %v", got, want)
		}
	}
}

func postDC(url, name string) (string, error) {
	return dc(http.MethodPost, url, name, "")
}

func updateDC(url, name, new string) (string, error) {
	return dc(http.MethodPut, url, new, name)
}

func dc(method, url, new, name string) (string, error) {
	buf := &bytes.Buffer{}
	dc := DC{Name: new}
	if err := json.NewEncoder(buf).Encode(&dc); err != nil {
		return "", errors.Wrap(err, "json encode")
	}

	url = url + "/v1/dc"
	if name != "" {
		url = fmt.Sprintf("%v/%v", url, name)
	}
	req, err := http.NewRequest(method, url, buf)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "post dc")
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
		I string `json:"dc_id"`
	}{}
	if err := json.NewDecoder(buf).Decode(&res); err != nil {
		return "", errors.Wrap(err, "json decode")
	}
	return res.I, nil
}

func getDCs(url, name string) ([]DC, error) {
	resp, err := http.Get(url + "/v1/dc")
	if err != nil {
		return nil, errors.Wrap(err, "get dc")
	}

	buf := &bytes.Buffer{}
	io.Copy(buf, resp.Body)
	resp.Body.Close()

	if got, want := resp.StatusCode, http.StatusOK; got != want {
		return nil, errors.Errorf("bad status: got %v, want %v, %v", got, want, buf.String())
	}

	r := map[string][]DC{}
	if err := json.NewDecoder(buf).Decode(&r); err != nil {
		return nil, errors.Wrap(err, "json decode")
	}
	if _, ok := r["results"]; !ok {
		return nil, errors.New("missing result key from response")
	}
	return r["results"], nil
}

func TestBadAddDC(t *testing.T) {
	mds := &mockDataStore{}
	store, err := GetTestEventStore(mds)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}

	ts := httptest.NewServer(NewServer(store, "", ""))

	name := ""

	_, err = postDC(ts.URL, name)
	if err == nil {
		t.Fatalf("should fail to post dc with empty name: %v", err)
	}
	if got, want := err.(jh.Error).Status(), http.StatusBadRequest; got != want {
		t.Fatalf("bad status: got %v, want %v", got, want)
	}
}

func TestBadUpdateDC(t *testing.T) {
	mds := &mockDataStore{}
	store, err := GetTestEventStore(mds)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}

	ts := httptest.NewServer(NewServer(store, "", ""))

	if _, err := postDC(ts.URL, "foo"); err != nil {
		t.Fatalf("initial post: %v", err)
	}

	_, err = updateDC(ts.URL, "foo", "")
	if err == nil {
		t.Fatalf("should fail to post dc with empty name: %v", err)
	}
	if got, want := err.(jh.Error).Status(), http.StatusBadRequest; got != want {
		t.Fatalf("bad status: got %v, want %v", got, want)
	}
}
