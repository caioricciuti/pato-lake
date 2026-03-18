package server

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/caioricciuti/pato-lake/internal/alerts"
	"github.com/caioricciuti/pato-lake/internal/config"
	"github.com/caioricciuti/pato-lake/internal/database"
	"github.com/caioricciuti/pato-lake/internal/duckdb"
	"github.com/caioricciuti/pato-lake/internal/governance"
	"github.com/caioricciuti/pato-lake/internal/ingest"
	"github.com/caioricciuti/pato-lake/internal/models"
	"github.com/caioricciuti/pato-lake/internal/pipelines"
	"github.com/caioricciuti/pato-lake/internal/scheduler"
	"github.com/caioricciuti/pato-lake/internal/server/handlers"
	"github.com/caioricciuti/pato-lake/internal/server/middleware"
	"github.com/go-chi/chi/v5"
)

// Server is the main HTTP server.
type Server struct {
	cfg            *config.Config
	db             *database.DB
	engine         *duckdb.Engine
	scheduler      *scheduler.Runner
	pipelineRunner *pipelines.Runner
	modelRunner    *models.Runner
	modelScheduler *models.Scheduler
	govSyncer      *governance.Syncer
	guardrails     *governance.GuardrailService
	alerts         *alerts.Dispatcher
	ingestBuffer   *ingest.Buffer
	router         chi.Router
	http           *http.Server
	frontendFS     fs.FS
}

// New creates a new Server with all routes configured.
func New(cfg *config.Config, db *database.DB, engine *duckdb.Engine, frontendFS fs.FS) *Server {
	r := chi.NewRouter()

	sched := scheduler.NewRunner(db, engine)
	pipeRunner := pipelines.NewRunner(db, engine, cfg)
	modelRunner := models.NewRunner(db, engine)
	modelScheduler := models.NewScheduler(db, modelRunner)

	govStore := governance.NewStore(db)
	govSyncer := governance.NewSyncer(govStore, db, engine)
	alertDispatcher := alerts.NewDispatcher(db, cfg)
	ingestBuf := ingest.NewBuffer(db, engine)

	s := &Server{
		cfg:            cfg,
		db:             db,
		engine:         engine,
		scheduler:      sched,
		pipelineRunner: pipeRunner,
		modelRunner:    modelRunner,
		modelScheduler: modelScheduler,
		govSyncer:      govSyncer,
		guardrails:     governance.NewGuardrailService(govStore, db),
		alerts:         alertDispatcher,
		ingestBuffer:   ingestBuf,
		router:         r,
		frontendFS:     frontendFS,
	}

	s.setupRoutes()

	s.http = &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 5 * time.Minute,
		IdleTimeout:  120 * time.Second,
	}

	return s
}

func (s *Server) setupRoutes() {
	r := s.router
	cfg := s.cfg
	db := s.db

	// Global middleware
	r.Use(middleware.Logger)
	r.Use(middleware.SecurityHeaders(!cfg.DevMode))
	r.Use(middleware.CORS(middleware.CORSConfig{
		DevMode:        cfg.DevMode,
		AllowedOrigins: cfg.AllowedOrigins,
		AppURL:         cfg.AppURL,
	}))

	// Health check
	healthHandler := &handlers.HealthHandler{}
	r.Get("/health", healthHandler.Health)

	// Rate limiter
	rateLimiter := middleware.NewRateLimiter(db)

	// Webhook endpoint for pipelines (token auth)
	r.Post("/api/pipelines/webhook/{id}", pipelines.HandleWebhook)

	// Event ingestion handler (shared across public + management routes)
	ingestHandler := &handlers.IngestHandler{DB: db, Config: cfg, Buffer: s.ingestBuffer}

	// API routes
	r.Route("/api", func(api chi.Router) {
		// Auth routes (no session required)
		authHandler := &handlers.AuthHandler{
			DB:          db,
			RateLimiter: rateLimiter,
			Config:      cfg,
		}
		api.Route("/auth", authHandler.Routes)

		// License status (no session required)
		licenseHandler := &handlers.LicenseHandler{DB: db, Config: cfg}
		api.Get("/license", licenseHandler.GetLicense)

		// Shared notebook route (no auth required)
		notebooksHandler := &handlers.NotebooksHandler{DB: db, Engine: s.engine, Config: cfg}
		notebooksHandler.SharedRoutes(api)

		// All routes below require a valid session (or API key)
		api.Group(func(protected chi.Router) {
			protected.Use(middleware.SessionOrAPIKey(db))

			// License activation
			protected.Post("/license/activate", licenseHandler.ActivateLicense)
			protected.Post("/license/deactivate", licenseHandler.DeactivateLicense)

			// Query execution
			queryHandler := &handlers.QueryHandler{DB: db, Engine: s.engine, Config: cfg, Guardrails: s.guardrails, GovStore: s.govSyncer.GetStore()}
			protected.Route("/query", queryHandler.Routes)

			// Saved queries
			savedQueriesHandler := &handlers.SavedQueriesHandler{DB: db}
			protected.Route("/saved-queries", savedQueriesHandler.Routes)

			// Dashboards
			dashboardsHandler := &handlers.DashboardsHandler{DB: db, Engine: s.engine, Config: cfg}
			protected.Mount("/dashboards", dashboardsHandler.Routes())

			// Pipelines
			pipelinesHandler := &handlers.PipelinesHandler{DB: db, Engine: s.engine, Config: cfg, Runner: s.pipelineRunner}
			protected.Mount("/pipelines", pipelinesHandler.Routes())

			// Models (dbt-like SQL transformations)
			modelsHandler := &handlers.ModelsHandler{DB: db, Engine: s.engine, Config: cfg, Runner: s.modelRunner}
			protected.Mount("/models", modelsHandler.Routes())

			// Brain AI assistant
			brainHandler := &handlers.BrainHandler{DB: db, Engine: s.engine, Config: cfg}
			protected.Route("/brain", brainHandler.Routes)

			// API keys
			apiKeysHandler := &handlers.APIKeysHandler{DB: db, Config: cfg}
			protected.Route("/keys", apiKeysHandler.Routes)

			// Ingest: source management + public event endpoint
			protected.Route("/ingest", func(ir chi.Router) {
				ingestHandler.ManagementRoutes(ir)
				// Public-ish ingestion endpoint lives here too (auth already applied by protected group)
				ingestHandler.PublicRoutes(ir)
			})

			// Notebooks
			protected.Mount("/notebooks", notebooksHandler.Routes())

			// Admin routes
			adminHandler := &handlers.AdminHandler{DB: db, Engine: s.engine, Config: cfg}
			protected.Route("/admin", func(ar chi.Router) {
				adminHandler.Routes(ar)
				ar.Route("/keys", apiKeysHandler.AdminRoutes)
			})

			// Pro-only features
			protected.Group(func(pro chi.Router) {
				pro.Use(middleware.RequirePro(cfg))

				// Scheduled jobs
				schedulesHandler := &handlers.SchedulesHandler{DB: db, Engine: s.engine, Config: cfg}
				pro.Route("/schedules", schedulesHandler.Routes)

				// Governance
				govHandler := &handlers.GovernanceHandler{
					DB: db, Engine: s.engine, Config: cfg,
					Store:  s.govSyncer.GetStore(),
					Syncer: s.govSyncer,
				}
				pro.Mount("/governance", govHandler.Routes())
			})
		})
	})

	// SPA fallback
	if s.frontendFS != nil {
		if _, err := s.frontendFS.Open("index.html"); err != nil {
			slog.Warn("Frontend assets not embedded; build the frontend first or use a release binary")
			r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprintln(w, "Frontend assets not available. Build the frontend first or use a release binary.")
			})
		} else {
			fileServer := http.FileServer(http.FS(s.frontendFS))
			r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
				path := r.URL.Path[1:]
				f, err := s.frontendFS.Open(path)
				if err != nil {
					w.Header().Set("Cache-Control", "no-cache")
					r.URL.Path = "/"
				} else {
					f.Close()
					if strings.HasPrefix(path, "assets/") {
						w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
					} else {
						w.Header().Set("Cache-Control", "no-cache")
					}
				}
				fileServer.ServeHTTP(w, r)
			})
		}
	}

	slog.Info("Routes configured")
}

// Start starts the HTTP server and background workers.
func (s *Server) Start() error {
	s.scheduler.Start()
	s.pipelineRunner.Start()
	s.modelScheduler.Start()
	s.govSyncer.StartBackground()
	s.alerts.Start()
	s.ingestBuffer.Start()

	slog.Info("Server listening", "addr", s.http.Addr)
	return s.http.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	slog.Info("Graceful shutdown initiated")
	s.scheduler.Stop()
	s.pipelineRunner.Stop()
	s.modelScheduler.Stop()
	s.govSyncer.Stop()
	s.alerts.Stop()
	s.ingestBuffer.Stop()
	return s.http.Shutdown(ctx)
}
