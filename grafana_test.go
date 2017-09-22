package eventmaster

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pkg/errors"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

func TestGrafanaAddDataSource(t *testing.T) {
	store, err := GetTestEventStore(NewNoOpDataStore())
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}
	ts := httptest.NewServer(NewServer(store, "", ""))
	resp, err := http.Get(ts.URL + "/grafana")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if got, want := resp.StatusCode, http.StatusOK; got != want {
		t.Fatalf("bad status: got %v, want %v", got, want)
	}
}

func TestGrafanaSearch(t *testing.T) {
	mds := &mockDataStore{}
	store, err := GetTestEventStore(mds)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}

	ts := httptest.NewServer(NewServer(store, "", ""))

	if _, err := grafSearch(ts.URL, "bad target"); err == nil {
		t.Fatalf("should have not been able to successfully get a bad target but did")
	}

	for _, target := range []string{"dc", "topic"} {
		dcs, err := grafSearch(ts.URL, target)
		if err != nil {
			t.Fatalf("search failure: %v", err)
		}
		if got, want := dcs[0], "all"; got != want {
			t.Fatalf("first entry was not 'all'; got: %v, want %v", got, want)
		}

		if got, want := len(dcs), 1; got != want {
			t.Fatalf("incorrect response lenght: got %v, want %v", got, want)
		}
	}

	if err := PopulateTestData(store); err != nil {
		t.Fatalf("populating test data: %v", err)
	}

	for _, target := range []string{"dc", "topic"} {
		dcs, err := grafSearch(ts.URL, target)
		if err != nil {
			t.Fatalf("search failure: %v", err)
		}
		if got, want := dcs[0], "all"; got != want {
			t.Fatalf("first entry was not 'all'; got: %v, want %v", got, want)
		}

		if got, want := len(dcs), 6; got != want {
			t.Fatalf("incorrect response lenght: got %v, want %v", got, want)
		}
	}
}

func grafSearch(url, target string) ([]string, error) {
	buf := &bytes.Buffer{}
	tr := TemplateRequest{Target: target}
	if err := json.NewEncoder(buf).Encode(&tr); err != nil {
		return nil, errors.Wrap(err, "json encode")
	}
	resp, err := http.Post(url+"/grafana/search", "application/json", buf)
	if err != nil {
		return nil, errors.Wrap(err, "search post")
	}

	buf.Reset()
	io.Copy(buf, resp.Body)
	resp.Body.Close()

	if got, want := resp.StatusCode, http.StatusOK; got != want {
		return nil, errors.Errorf("bad status: got %v, want %v, %v", got, want, buf.String())
	}

	dcs := []string{}
	if err := json.NewDecoder(buf).Decode(&dcs); err != nil {
		return nil, errors.Wrap(err, "json decode")
	}
	return dcs, nil
}

func TestGrafanaAnnotations(t *testing.T) {
	mds := &mockDataStore{}
	store, err := GetTestEventStore(mds)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}

	ts := httptest.NewServer(NewServer(store, "", ""))

	if err := PopulateTestData(store); err != nil {
		t.Fatalf("populating test data: %v", err)
	}

	topics, err := mds.GetTopics()
	if err != nil {
		t.Fatalf("get topics: %v", err)
	}
	dcs, err := mds.GetDcs()
	if err != nil {
		t.Fatalf("get dcs: %v", err)
	}

	n := time.Now()
	host := "h0"
	var i int64
	for _, topic := range topics {
		for _, dc := range dcs {
			et := n.Unix() + i
			e := &UnaddedEvent{
				Host:      host,
				Dc:        dc.Name,
				TopicName: topic.Name,
				EventTime: et,
			}
			if _, err := store.AddEvent(e); err != nil {
				t.Fatalf("adding event: %v", err)
			}
			i++
		}
	}

	tests := []struct {
		label string
		ar    AnnotationsReq
		count int
	}{
		{
			label: "all",
			ar: AnnotationsReq{
				Range: Range{
					From: n.Add(-10 * time.Second),

					// well outside the window:
					To: n.Add(time.Duration(i) * time.Second),
				},
			},
			count: 25,
		},
		{
			label: "trim time to",
			ar: AnnotationsReq{
				Range: Range{
					From: n.Add(-10 * time.Second),
					To:   n.Add(time.Duration(i/2) * time.Second),
				},
			},
			count: 12,
		},
		{
			label: "trim time from",
			ar: AnnotationsReq{
				Range: Range{
					From: n.Add(time.Duration(i/2) * time.Second),
					To:   n.Add(time.Duration(i) * time.Second),
				},
			},
			count: 12,
		},
		{
			label: "one dc",
			ar: AnnotationsReq{
				Range: Range{
					From: n.Add(-10 * time.Second),
					To:   n.Add(time.Duration(i) * time.Second),
				},
				Annotation: Annotation{
					Query: `{"dc": "dc0000"}`,
				},
			},
			count: 5,
		},
		{
			label: "one topic",
			ar: AnnotationsReq{
				Range: Range{
					From: n.Add(-10 * time.Second),
					To:   n.Add(time.Duration(i) * time.Second),
				},
				Annotation: Annotation{
					Query: `{"topic": "t0000"}`,
				},
			},
			count: 5,
		},
		{
			label: "one dc and topic",
			ar: AnnotationsReq{
				Range: Range{
					From: n.Add(-10 * time.Second),
					To:   n.Add(time.Duration(i) * time.Second),
				},
				Annotation: Annotation{
					Query: `{"topic": "t0000", "dc": "dc0000"}`,
				},
			},
			count: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.label, func(t *testing.T) {
			anns, err := grafAnnotations(ts.URL, test.ar)
			if err != nil {
				t.Fatalf("getting annotations for %v: %v", test.ar, err)
			}
			if got, want := len(anns), test.count; got != want {
				t.Fatalf("unexpected quantity of annotations: got %d, want %d", got, want)
			}
		})
	}
}

func grafAnnotations(url string, req AnnotationsReq) ([]AnnotationResponse, error) {
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(&req); err != nil {
		return nil, errors.Wrap(err, "json encode")
	}
	resp, err := http.Post(url+"/grafana/annotations", "application/json", buf)
	if err != nil {
		return nil, errors.Wrap(err, "annotations post")
	}

	buf.Reset()
	io.Copy(buf, resp.Body)
	resp.Body.Close()

	if got, want := resp.StatusCode, http.StatusOK; got != want {
		return nil, errors.Errorf("bad status: got %v, want %v, %v", got, want, buf.String())
	}

	ar := []AnnotationResponse{}
	if err := json.NewDecoder(buf).Decode(&ar); err != nil {
		return nil, errors.Wrap(err, "json decode")
	}
	return ar, nil
}
