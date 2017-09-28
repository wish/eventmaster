package eventmaster

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

var Version = "unset"
var Git = "unset"

func (h *Server) version(w http.ResponseWriter, req *http.Request, p httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	r := struct {
		Version string `json:"version"`
		Git     string `json:"git"`
	}{
		Version: Version,
		Git:     Git,
	}
	if err := json.NewEncoder(w).Encode(&r); err != nil {
		log.Printf("json encode: %+v", err)
	}
}
