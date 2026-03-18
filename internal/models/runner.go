package models

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/caioricciuti/pato-lake/internal/database"
	"github.com/caioricciuti/pato-lake/internal/duckdb"
)

// Runner executes model builds against DuckDB.
type Runner struct {
	db     *database.DB
	engine *duckdb.Engine
	mu     sync.Mutex // prevents concurrent runs
	running bool
}

// NewRunner creates a new model runner.
func NewRunner(db *database.DB, engine *duckdb.Engine) *Runner {
	return &Runner{
		db:     db,
		engine: engine,
	}
}

// RunAll executes all models in dependency order.
func (r *Runner) RunAll(triggeredBy string) (string, error) {
	if err := r.acquireLock(); err != nil {
		return "", err
	}
	defer r.releaseLock()

	allModels, err := r.db.GetModels()
	if err != nil {
		return "", fmt.Errorf("load models: %w", err)
	}
	if len(allModels) == 0 {
		return "", fmt.Errorf("no models defined")
	}

	dag, idToModel, modelTargets, err := r.buildDAG(allModels)
	if err != nil {
		return "", err
	}

	return r.execute(triggeredBy, dag, idToModel, modelTargets)
}

// RunPipeline executes only the connected component containing anchorModelID.
func (r *Runner) RunPipeline(anchorModelID, triggeredBy string) (string, error) {
	if err := r.acquireLock(); err != nil {
		return "", err
	}
	defer r.releaseLock()

	allModels, err := r.db.GetModels()
	if err != nil {
		return "", fmt.Errorf("load models: %w", err)
	}
	if len(allModels) == 0 {
		return "", fmt.Errorf("no models defined")
	}

	dag, idToModel, modelTargets, err := r.buildDAG(allModels)
	if err != nil {
		return "", err
	}

	component := dag.ComponentContaining(anchorModelID)
	if len(component) == 0 {
		return "", fmt.Errorf("anchor model not found in DAG")
	}

	dag.Order = component
	return r.execute(triggeredBy, dag, idToModel, modelTargets)
}

// RunSingle executes a single model and its upstream dependencies.
func (r *Runner) RunSingle(modelID, triggeredBy string) (string, error) {
	if err := r.acquireLock(); err != nil {
		return "", err
	}
	defer r.releaseLock()

	allModels, err := r.db.GetModels()
	if err != nil {
		return "", fmt.Errorf("load models: %w", err)
	}

	dag, idToModel, modelTargets, err := r.buildDAG(allModels)
	if err != nil {
		return "", err
	}

	// Filter to only the target model and its upstream deps
	upstream := GetUpstreamDeps(modelID, dag.Deps)
	upstream[modelID] = true

	var filteredIDs []string
	for _, id := range dag.Order {
		if upstream[id] {
			filteredIDs = append(filteredIDs, id)
		}
	}
	dag.Order = filteredIDs

	return r.execute(triggeredBy, dag, idToModel, modelTargets)
}

// Validate checks all models for reference errors and cycles.
func (r *Runner) Validate() ([]ValidationError, error) {
	allModels, err := r.db.GetModels()
	if err != nil {
		return nil, fmt.Errorf("load models: %w", err)
	}
	if len(allModels) == 0 {
		return nil, nil
	}

	nameToID := make(map[string]string)
	for _, m := range allModels {
		nameToID[m.Name] = m.ID
	}

	var errors []ValidationError
	refsByID := make(map[string][]string)

	for _, m := range allModels {
		refs := ExtractRefs(m.SQLBody)
		refsByID[m.ID] = refs
		for _, ref := range refs {
			if _, ok := nameToID[ref]; !ok {
				errors = append(errors, ValidationError{
					ModelID:   m.ID,
					ModelName: m.Name,
					Error:     fmt.Sprintf("references unknown model %q via $ref()", ref),
				})
			}
			if nameToID[ref] == m.ID {
				errors = append(errors, ValidationError{
					ModelID:   m.ID,
					ModelName: m.Name,
					Error:     fmt.Sprintf("cannot reference itself via $ref(%s)", ref),
				})
			}
		}
	}

	if len(errors) > 0 {
		return errors, nil
	}

	// Check for cycles
	var modelIDs []string
	for _, m := range allModels {
		modelIDs = append(modelIDs, m.ID)
	}

	_, dagErr := BuildDAG(modelIDs, refsByID, nameToID)
	if dagErr != nil {
		errors = append(errors, ValidationError{
			Error: dagErr.Error(),
		})
	}

	return errors, nil
}

// ValidationError represents a validation problem.
type ValidationError struct {
	ModelID   string `json:"model_id,omitempty"`
	ModelName string `json:"model_name,omitempty"`
	Error     string `json:"error"`
}

// -- Internal helpers ------------------------------------------------

func (r *Runner) buildDAG(allModels []database.Model) (*DepGraph, map[string]database.Model, map[string]string, error) {
	nameToID := make(map[string]string)
	idToModel := make(map[string]database.Model)
	modelTargets := make(map[string]string)
	var modelIDs []string
	refsByID := make(map[string][]string)

	for _, m := range allModels {
		nameToID[m.Name] = m.ID
		idToModel[m.ID] = m
		modelTargets[m.Name] = m.TargetDatabase
		modelIDs = append(modelIDs, m.ID)
		refsByID[m.ID] = ExtractRefs(m.SQLBody)
	}

	dag, err := BuildDAG(modelIDs, refsByID, nameToID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("build DAG: %w", err)
	}

	return dag, idToModel, modelTargets, nil
}

func (r *Runner) execute(triggeredBy string, dag *DepGraph, idToModel map[string]database.Model, modelTargets map[string]string) (string, error) {
	runID, err := r.db.CreateModelRun(len(dag.Order), triggeredBy)
	if err != nil {
		return "", fmt.Errorf("create run: %w", err)
	}

	// Create pending result records
	for _, id := range dag.Order {
		m := idToModel[id]
		if _, err := r.db.CreateModelRunResult(runID, m.ID, m.Name); err != nil {
			slog.Error("Failed to create run result", "model", m.Name, "error", err)
		}
	}

	// Execute in topological order
	failed := make(map[string]bool)
	var succeeded, failedCount, skipped int

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for _, id := range dag.Order {
		m := idToModel[id]

		// Skip if any upstream dependency failed
		shouldSkip := false
		for _, depID := range dag.Deps[id] {
			if failed[depID] {
				shouldSkip = true
				break
			}
		}

		if shouldSkip {
			skipped++
			failed[id] = true
			r.db.UpdateModelRunResult(runID, id, "skipped", "", 0, "upstream dependency failed")
			r.db.UpdateModelStatus(id, "error", "upstream dependency failed")
			continue
		}

		// Resolve $ref()
		resolvedSQL, resolveErr := ResolveRefs(m.SQLBody, modelTargets)
		if resolveErr != nil {
			failedCount++
			failed[id] = true
			r.db.UpdateModelRunResult(runID, id, "error", resolvedSQL, 0, resolveErr.Error())
			r.db.UpdateModelStatus(id, "error", resolveErr.Error())
			continue
		}

		// Mark as running
		r.db.UpdateModelRunResult(runID, id, "running", "", 0, "")

		// Build and execute DDL
		stmts := buildDDL(m, resolvedSQL)
		start := time.Now()
		var execErr error

		for _, stmt := range stmts {
			_, execErr = r.engine.Exec(ctx, stmt)
			if execErr != nil {
				break
			}
		}

		elapsed := time.Since(start).Milliseconds()
		ddlForLog := stmts[len(stmts)-1] // log the main statement

		if execErr != nil {
			failedCount++
			failed[id] = true
			r.db.UpdateModelRunResult(runID, id, "error", ddlForLog, elapsed, execErr.Error())
			r.db.UpdateModelStatus(id, "error", execErr.Error())
			slog.Error("Model execution failed", "model", m.Name, "error", execErr)
		} else {
			succeeded++
			r.db.UpdateModelRunResult(runID, id, "success", ddlForLog, elapsed, "")
			r.db.UpdateModelStatus(id, "success", "")
		}
	}

	// Finalize run
	runStatus := "success"
	if failedCount > 0 && succeeded > 0 {
		runStatus = "partial"
	} else if failedCount > 0 || skipped == len(dag.Order) {
		runStatus = "error"
	}
	r.db.FinalizeModelRun(runID, runStatus, succeeded, failedCount, skipped)

	return runID, nil
}

// buildDDL generates the DDL statement(s) for a model.
// Returns a slice because TABLE needs DROP + CREATE as separate statements.
func buildDDL(m database.Model, resolvedSQL string) []string {
	switch m.Materialization {
	case "table":
		drop := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s"`, m.TargetDatabase, m.Name)
		create := fmt.Sprintf(`CREATE TABLE "%s"."%s" AS %s`,
			m.TargetDatabase, m.Name, resolvedSQL)
		return []string{drop, create}
	default: // view
		return []string{
			fmt.Sprintf(`CREATE OR REPLACE VIEW "%s"."%s" AS %s`,
				m.TargetDatabase, m.Name, resolvedSQL),
		}
	}
}

func (r *Runner) acquireLock() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running {
		return fmt.Errorf("a model run is already in progress")
	}
	r.running = true
	return nil
}

func (r *Runner) releaseLock() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.running = false
}
