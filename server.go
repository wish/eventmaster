package eventmaster

import (
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	assetfs "github.com/elazarl/go-bindata-assetfs"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	tmpl "github.com/ContextLogic/eventmaster/templates"
	"github.com/ContextLogic/eventmaster/ui"
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

	hndlr := registerRoutes(srv)
	srv.handler = timer{hndlr}

	return srv
}

func registerRoutes(srv *Server) http.Handler {
	r := httprouter.New()

	// API endpoints
	r.POST("/v1/event", srv.handleAddEvent)
	r.GET("/v1/event", srv.handleGetEvent)
	r.GET("/v1/event/:id", srv.handleGetEventByID)
	r.POST("/v1/topic", srv.handleAddTopic)
	r.PUT("/v1/topic/:name", srv.handleUpdateTopic)
	r.GET("/v1/topic", srv.handleGetTopic)
	r.DELETE("/v1/topic/:name", srv.handleDeleteTopic)
	r.POST("/v1/dc", srv.handleAddDC)
	r.PUT("/v1/dc/:name", srv.handleUpdateDC)
	r.GET("/v1/dc", srv.handleGetDC)

	r.GET("/v1/health", srv.handleHealthCheck)

	// GitHub webhook endpoint
	r.POST("/v1/github_event", srv.handleGitHubEvent)

	// UI endpoints
	r.GET("/", srv.HandleMainPage)
	r.GET("/add_event", srv.HandleCreatePage)
	r.GET("/topic", srv.HandleTopicPage)
	r.GET("/dc", srv.HandleDCPage)
	r.GET("/event", srv.HandleGetEventPage)

	// grafana datasource endpoints
	r.GET("/grafana", cors(srv.grafanaOK))
	r.GET("/grafana/", cors(srv.grafanaOK))
	r.OPTIONS("/grafana/:route", cors(srv.grafanaOK))
	r.POST("/grafana/annotations", cors(srv.grafanaAnnotations))
	r.POST("/grafana/search", cors(srv.grafanaSearch))

	r.Handler("GET", "/metrics", promhttp.Handler())

	r.Handler("GET", "/ui/*filepath", http.FileServer(srv.ui))

	return r
}

type timer struct {
	http.Handler
}

func (t timer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	defer func() {
		httpReqLatencies.WithLabelValues(req.URL.Path).Observe(float64(time.Since(start) / time.Microsecond))
		reqLatency.WithLabelValues(req.URL.Path).Observe(float64(time.Since(start) / time.Microsecond))
	}()

	lw := NewStatusRecorder(w)
	t.Handler.ServeHTTP(lw, req)

	httpReqCounter.WithLabelValues(req.URL.Path).Inc()
	httpRespCounter.WithLabelValues(req.URL.Path, fmt.Sprintf("%d", round(lw.Status()))).Inc()
}

func round(i int) int {
	return i - i%100
}
