package main

import (
    "fmt"
    "net/http"
    "html/template"
)

type mainPageHandler struct {
    store *EventStore
}

func (mph *mainPageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    t, err := template.New("main.html").ParseFiles("templates/main.html")
    if err != nil {
        http.Error(w, fmt.Sprintf("error parsing template main.html: %v", err), http.StatusInternalServerError)
        return
    }
    err = t.Execute(w, "")
    if err != nil {
        http.Error(w, fmt.Sprintf("error executing template: %v", err), http.StatusInternalServerError)
    }
}