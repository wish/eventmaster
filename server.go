package eventmaster

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"time"

	assetfs "github.com/elazarl/go-bindata-assetfs"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"

	"github.com/wish/eventmaster/jh"
	"github.com/wish/eventmaster/metrics"
	tmpl "github.com/wish/eventmaster/templates"
	"github.com/wish/eventmaster/ui"
)

// Server implements http.Handler for the eventmaster http server.
type Server struct {
	store *EventStore

	handler http.Handler

	ui        http.FileSystem
	templates TemplateGetter
}

// ServeHTTP dispatches to the underlying router.
func (srv *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	srv.handler.ServeHTTP(w, req)
}

// NewServer returns a ready-to-use Server that uses store, and the appropriate
// static and templates facilities.
//
// If static or templates are non-empty then files are served from those
// locations (useful for development). Otherwise the server uses embedded
// static assets.
func NewServer(store *EventStore, static, templates string) *Server {
	// Handle static files either embedded (empty static) or off the filesystem (during dev work)
	var fs http.FileSystem
	switch static {
	case "":
		fs = &assetfs.AssetFS{
			Asset:     ui.Asset,
			AssetDir:  ui.AssetDir,
			AssetInfo: ui.AssetInfo,
		}
	default:
		if p, d := filepath.Split(static); d == "ui" {
			static = p
		}
		fs = http.Dir(static)
	}

	var t TemplateGetter
	switch templates {
	case "":
		t = NewAssetTemplate(tmpl.Asset)
	default:
		t = Disk{Root: templates}
	}

	srv := &Server{
		store:     store,
		ui:        fs,
		templates: t,
	}

	srv.handler = registerRoutes(srv)

	return srv
}

func registerRoutes(srv *Server) http.Handler {
	r := httprouter.New()

	// API endpoints
	r.POST("/v1/event", latency("/v1/event", jh.Adapter(srv.addEvent)))
	r.GET("/v1/event", latency("/v1/event", jh.Adapter(srv.getEvent)))
	r.GET("/v1/event/:id", latency("/v1/event", jh.Adapter(srv.getEventByID)))
	r.POST("/v1/topic", latency("/v1/topic", jh.Adapter(srv.addTopic)))
	r.PUT("/v1/topic/:name", latency("/v1/topic", jh.Adapter(srv.updateTopic)))
	r.GET("/v1/topic", latency("/v1/topic", jh.Adapter(srv.getTopic)))
	r.DELETE("/v1/topic/:name", latency("/v1/topic", jh.Adapter(srv.deleteTopic)))
	r.POST("/v1/dc", latency("/v1/dc", jh.Adapter(srv.addDC)))
	r.PUT("/v1/dc/:name", latency("/v1/dc", jh.Adapter(srv.updateDC)))
	r.GET("/v1/dc", latency("/v1/dc", jh.Adapter(srv.getDC)))

	r.GET("/v1/health", latency("/v1/health", jh.Adapter(srv.healthCheck)))

	// GitHub webhook endpoint
	r.POST("/v1/github_event", latency("/v1/github_event", jh.Adapter(srv.gitHubEvent)))

	// UI endpoints
	r.GET("/", latency("/", srv.HandleMainPage))
	r.GET("/add_event", latency("/add_event", srv.HandleCreatePage))
	r.GET("/topic", latency("/topic", srv.HandleTopicPage))
	r.GET("/dc", latency("/dc", srv.HandleDCPage))
	r.GET("/event", latency("/event", srv.HandleGetEventPage))

	// grafana datasource endpoints
	r.GET("/grafana", latency("/grafana", cors(srv.grafanaOK)))
	r.GET("/grafana/", latency("/grafana/", cors(srv.grafanaOK)))
	r.OPTIONS("/grafana/:route", latency("/grafana", cors(srv.grafanaOK)))
	r.POST("/grafana/annotations", latency("/grafana/annotations", cors(srv.grafanaAnnotations)))
	r.POST("/grafana/search", latency("/grafana/search", cors(srv.grafanaSearch)))

	r.Handler("GET", "/metrics", promhttp.Handler())

	r.Handler("GET", "/ui/*filepath", http.FileServer(srv.ui))

	r.GET("/version/", latency("/version/", srv.version))

	r.PanicHandler = func(w http.ResponseWriter, req *http.Request, i interface{}) {
		metrics.HTTPStatus("panic", http.StatusInternalServerError)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		e := struct {
			E string `json:"error"`
		}{"server panicked"}
		if err := json.NewEncoder(w).Encode(&e); err != nil {
			log.Printf("json encode: %v", err)
		}
	}

	r.NotFound = func(w http.ResponseWriter, req *http.Request) {
		log.Printf("not found: %+v", req.URL.Path)
		metrics.HTTPStatus(http.StatusText(http.StatusNotFound), http.StatusNotFound)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		e := struct {
			E string `json:"error"`
		}{"not found"}
		if err := json.NewEncoder(w).Encode(&e); err != nil {
			log.Printf("json encode: %v", err)
		}
	}

	r.MethodNotAllowed = func(w http.ResponseWriter, req *http.Request) {
		metrics.HTTPStatus(http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		e := struct {
			E string `json:"error"`
		}{"method not allowed"}
		if err := json.NewEncoder(w).Encode(&e); err != nil {
			log.Printf("json encode: %v", err)
		}
	}

	return r
}

func latency(prefix string, h httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		start := time.Now()
		defer func() {
			metrics.HTTPLatency(prefix, start)
		}()

		lw := NewStatusRecorder(w)
		h(lw, req, ps)

		metrics.HTTPStatus(prefix, lw.Status())
	}
}

func (srv *Server) healthCheck(w http.ResponseWriter, r *http.Request, _ httprouter.Params) (interface{}, error) {
	return nil, nil
}
