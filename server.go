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

	srv.handler = registerRoutes(srv)

	return srv
}

func registerRoutes(srv *Server) http.Handler {
	r := httprouter.New()

	// API endpoints
	r.POST("/v1/event", latency("/v1/event", srv.handleAddEvent))
	r.GET("/v1/event", latency("/v1/event", srv.handleGetEvent))
	r.GET("/v1/event/:id", latency("/v1/event", srv.handleGetEventByID))
	r.POST("/v1/topic", latency("/v1/topic", srv.handleAddTopic))
	r.PUT("/v1/topic/:name", latency("/v1/topic", srv.handleUpdateTopic))
	r.GET("/v1/topic", latency("/v1/topic", srv.handleGetTopic))
	r.DELETE("/v1/topic/:name", latency("/v1/topic", srv.handleDeleteTopic))
	r.POST("/v1/dc", latency("/v1/dc", srv.handleAddDC))
	r.PUT("/v1/dc/:name", latency("/v1/dc", srv.handleUpdateDC))
	r.GET("/v1/dc", latency("/v1/dc", srv.handleGetDC))

	r.GET("/v1/health", latency("/v1/health", srv.handleHealthCheck))

	// GitHub webhook endpoint
	r.POST("/v1/github_event", latency("/v1/github_event", srv.handleGitHubEvent))

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

	return r
}

func latency(prefix string, h httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		start := time.Now()
		defer func() {
			httpReqLatencies.WithLabelValues(prefix).Observe(msSince(start))
		}()

		lw := NewStatusRecorder(w)
		h(lw, req, ps)

		httpRespCounter.WithLabelValues(prefix, fmt.Sprintf("%d", bucketHTTPStatus(lw.Status()))).Inc()
	}
}

// bucketHTTPStatus rounds down to the nearest hundred to facilitate categorizing http statuses.
func bucketHTTPStatus(i int) int {
	return i - i%100
}
