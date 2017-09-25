package eventmaster

import (
	"fmt"
	"html/template"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"

	eventmaster "github.com/ContextLogic/eventmaster/proto"
)

var funcMap = template.FuncMap{
	"getCommaSeparated": func(strs []string) template.HTML {
		return template.HTML(strings.Join(strs, ","))
	},
	"getChecked": func(val bool) template.HTMLAttr {
		if val == true {
			return template.HTMLAttr("checked")
		}
		return template.HTMLAttr("")
	},
	"getSelectedTopic": func(topics []string, name string) template.HTMLAttr {
		for _, topic := range topics {
			if topic == name {
				return template.HTMLAttr(`selected="selected"`)
			}
		}
		return template.HTMLAttr("")
	},
}

type GetEventPageData struct {
	Topics []Topic
	Query  *eventmaster.Query
}

func executeTemplate(w http.ResponseWriter, t *template.Template, data interface{}) {
	if err := t.Execute(w, data); err != nil {
		fmt.Println("Error executing template:", err)
	}
}

func (s *Server) HandleMainPage(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if r.URL.RawQuery == "" {
		http.Redirect(w, r, "/event", 301)
	} else {
		http.Redirect(w, r, "/event?"+r.URL.RawQuery, 301)
	}
}

func (s *Server) HandleGetEventPage(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	q, err := getQueryFromRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	topics, err := s.store.GetTopics()
	if err != nil {
		s.sendError(w, http.StatusInternalServerError, err, "Error getting topics from store, r.URL.Path", r.URL.Path)
		return
	}
	getEventQuery := GetEventPageData{
		Topics: topics,
		Query:  q,
	}

	t, err := s.templates.Get("query_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	executeTemplate(w, t, getEventQuery)
}

func (s *Server) HandleCreatePage(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	t, err := s.templates.Get("create_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	executeTemplate(w, t, nil)
}

func (s *Server) HandleTopicPage(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	t, err := s.templates.Get("topic_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	executeTemplate(w, t, nil)
}

func (s *Server) HandleDcPage(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	t, err := s.templates.Get("dc_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	executeTemplate(w, t, nil)
}
