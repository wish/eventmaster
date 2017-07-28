package main

import (
	"fmt"
	"html/template"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func executeTemplate(w http.ResponseWriter, t *template.Template, data interface{}) {
	if err := t.Execute(w, data); err != nil {
		http.Error(w, fmt.Sprintf("error executing template: %v", err), http.StatusInternalServerError)
	}
}

func (h *httpHandler) HandleMainPage(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	topics, err := h.store.GetTopics()
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, err, "Error getting topics from store", r.URL.Path)
		return
	}

	t, err := template.New("main.html").ParseFiles("ui/templates/main.html", "ui/templates/query_form.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
		return
	}
	executeTemplate(w, t, topics)
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
