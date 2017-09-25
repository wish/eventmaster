package eventmaster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"

	eventmaster "github.com/ContextLogic/eventmaster/proto"
)

// AnnotationsReq encodes the information provided by Grafana in its requests.
type AnnotationsReq struct {
	Range      Range      `json:"range"`
	Annotation Annotation `json:"annotation"`
}

// Range specifies the time range the request is valid for.
type Range struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

// Annotation is the object passed by Grafana when it fetches annotations.
//
// http://docs.grafana.org/plugins/developing/datasources/#annotation-query
type Annotation struct {
	// Name must match in the request and response
	Name string `json:"name"`

	Datasource string `json:"datasource"`
	IconColor  string `json:"iconColor"`
	Enable     bool   `json:"enable"`
	ShowLine   bool   `json:"showLine"`
	Query      string `json:"query"`
}

// AnnotationResponse contains all the information needed to render an
// annotation event.
//
// https://github.com/grafana/simple-json-datasource#annotation-api
type AnnotationResponse struct {
	// The original annotation sent from Grafana.
	Annotation Annotation `json:"annotation"`
	// Time since UNIX Epoch in milliseconds. (required)
	Time int64 `json:"time"`
	// The title for the annotation tooltip. (required)
	Title string `json:"title"`
	// Tags for the annotation. (optional)
	Tags string `json:"tags"`
	// Text for the annotation. (optional)
	Text string `json:"text"`
}

// AnnotationQuery is a collection of possible filters for a Grafana annotation
// request.
//
// These values are set in gear icon > annotations > edit > Query and can have
// such useful values as:
//
//		{"topic": "$topic", "dc": "$dc"}
//
// or if you do not want filtering leave it blank.
type AnnotationQuery struct {
	Topic string `json:"topic"`
	DC    string `json:"dc"`
}

type TemplateRequest struct {
	Target string `json:"target"`
}

// cors adds headers that Grafana requires to work as a direct access data
// source.
//
// forgetting to add these manifests itself as an unintellible error when
// adding a datasource.
//
// These are not required if using "proxy" access.
func cors(h httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		w.Header().Set("Access-Control-Allow-Headers", "accept, content-type")
		w.Header().Set("Access-Control-Allow-Methods", "POST")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		h(w, r, p)
	}
}

func (h *Server) grafanaOK(w http.ResponseWriter, r *http.Request, p httprouter.Params) {}

// grafanaAnnotations handles the POST requests from Grafana asking for
// a window of annotations.
//
// It accepts an AnnotationsReq in the request body, and parses the
// AnnotationsReq.Query for a AnnotationQuery. If one is found it parses it,
// and interprets the value "all" as "do not apply this filter".
func (h *Server) grafanaAnnotations(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	switch r.Method {
	case http.MethodPost:
		ar := AnnotationsReq{}
		if err := json.NewDecoder(r.Body).Decode(&ar); err != nil {
			http.Error(w, fmt.Sprintf("json decode failure: %v", err), http.StatusBadRequest)
			return
		}

		q := &eventmaster.Query{
			StartEventTime: ar.Range.From.Unix(),
			EndEventTime:   ar.Range.To.Unix(),
		}

		if rq := ar.Annotation.Query; rq != "" {
			aq := AnnotationQuery{}
			if err := json.Unmarshal([]byte(rq), &aq); err != nil {
				http.Error(w, fmt.Sprintf("json decode failure: %v", err), http.StatusBadRequest)
				return
			}
			if aq.DC != "" && aq.DC != "all" {
				q.Dc = []string{aq.DC}
			}
			if aq.Topic != "" && aq.Topic != "all" {
				q.TopicName = []string{aq.Topic}
			}
		}

		evs, err := h.store.Find(q)
		if err != nil {
			e := errors.Wrapf(err, "grafana search with %v", q)
			http.Error(w, e.Error(), http.StatusInternalServerError)
			return
		}

		ars := []AnnotationResponse{}
		for _, ev := range evs {
			ar, err := FromEvent(h.store, ev)
			if err != nil {
				http.Error(w, errors.Wrap(err, "from event").Error(), http.StatusInternalServerError)
				return
			}
			ars = append(ars, ar)
		}

		if err := json.NewEncoder(w).Encode(ars); err != nil {
			log.Printf("json enc: %+v", err)
		}
	default:
		http.Error(w, "bad method; supported POST", http.StatusBadRequest)
		return
	}
}

func (h *Server) grafanaSearch(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	req := TemplateRequest{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, errors.Wrap(err, "json decode").Error(), http.StatusBadRequest)
		return
	}

	tags := []string{"all"}
	switch req.Target {
	case "dc":
		dcs, err := h.store.GetDcs()
		if err != nil {
			http.Error(w, errors.Wrap(err, "get dcs").Error(), http.StatusInternalServerError)
			return
		}
		for _, dc := range dcs {
			tags = append(tags, dc.Name)
		}
	case "topic":
		topics, err := h.store.GetTopics()
		if err != nil {
			http.Error(w, errors.Wrap(err, "get topics").Error(), http.StatusInternalServerError)
			return
		}
		for _, topic := range topics {
			tags = append(tags, topic.Name)
		}
	default:
		http.Error(w, fmt.Sprintf("unknown target: got %q, want [%q, %q]", req.Target, "dc", "topic"), http.StatusBadRequest)
		return
	}

	sort.Strings(tags[1:])
	if err := json.NewEncoder(w).Encode(tags); err != nil {
		log.Printf("json encode failure: %+v", err)
	}
}

type topicNamer interface {
	getTopicName(string) string
	getDcName(string) string
}

func FromEvent(store topicNamer, ev *Event) (AnnotationResponse, error) {
	fm := template.FuncMap{
		"trim": strings.TrimSpace,
	}
	t := `<pre>
Host: {{ .Host }}
Target Hosts:
{{- range .TargetHosts }}
	{{ trim . -}}
{{ end }}
User: {{ .User }}
Data: {{ .Data }}
</pre>
`
	tmpl, err := template.New("text").Funcs(fm).Parse(t)
	if err != nil {
		return AnnotationResponse{}, errors.Wrap(err, "making template")
	}
	buf := &bytes.Buffer{}
	tmpl.Execute(buf, ev)
	r := AnnotationResponse{
		Time:  ev.EventTime * 1000,
		Title: fmt.Sprintf("%v in %v", store.getTopicName(ev.TopicID), store.getDcName(ev.DcID)),
		Text:  buf.String(),
		Tags:  strings.Join(ev.Tags, ","),
	}
	return r, nil
}
