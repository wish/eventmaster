package main

import (
	"fmt"
	"html/template"
	"net/http"
	"strings"

	"github.com/ContextLogic/eventmaster/eventmaster"
	"github.com/julienschmidt/httprouter"
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
	Topics []TopicData
	Query  *eventmaster.Query
}

func executeTemplate(w http.ResponseWriter, t *template.Template, data interface{}) {
	if err := t.Execute(w, data); err != nil {
		fmt.Println("Error executing template:", err)
	}
}

func (h *httpHandler) HandleMainPage(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if r.URL.RawQuery == "" {
		http.Redirect(w, r, "/event", 301)
	} else {
		http.Redirect(w, r, "/event?"+r.URL.RawQuery, 301)
	}
}

func (h *httpHandler) HandleGetEventPage(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	q, err := getQueryFromRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	topics, err := h.store.GetTopics()
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, err, "Error getting topics from store, r.URL.Path", r.URL.Path)
		return
	}
	getEventQuery := GetEventPageData{
		Topics: topics,
		Query:  q,
	}

	t, err := template.New("main.html").Funcs(funcMap).ParseFiles("ui/templates/main.html", "ui/templates/query_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	executeTemplate(w, t, getEventQuery)
}

func (h *httpHandler) HandleCreatePage(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	t, err := template.New("main.html").ParseFiles("ui/templates/main.html", "ui/templates/create_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	executeTemplate(w, t, nil)
}

func (h *httpHandler) HandleTopicPage(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	t, err := template.New("main.html").ParseFiles("ui/templates/main.html", "ui/templates/topic_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	executeTemplate(w, t, nil)
}

func (h *httpHandler) HandleDcPage(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	t, err := template.New("main.html").ParseFiles("ui/templates/main.html", "ui/templates/dc_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	executeTemplate(w, t, nil)
}
