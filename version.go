package eventmaster

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/julienschmidt/httprouter"
)

// Version maps to tagged releases of the software.
var Version = "unset"

// Git stores the commit used during build.
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

// PrintVersions writes version information for the running binary to stdout.
func PrintVersions() {
	cmd := os.Args[0]
	fmt.Printf("%v\n", cmd)
	fmt.Printf("version: %v\n", Version)
	fmt.Printf("git@%v\n", Git)
}
