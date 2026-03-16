package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
)

var (
	graphdbURL    string
	graphdbRepo   string
	rpcURL        string
	dataSource    string
	duneClient    *DuneClient
	graphExplorer *GraphExplorer

	jobMu    sync.RWMutex
	jobState graphJobState
)

type graphBuildRequest struct {
	Address string `json:"address"`
	Depth   int    `json:"depth"`
	Blocks  int    `json:"blocks"`
	Days    int    `json:"days"`
}

type graphJobState struct {
	Running    bool
	Progress   int
	Result     *TxGraph
	Error      string
	Request    graphBuildRequest
	StartedAt  time.Time
	FinishedAt time.Time
}

func graphHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		startGraphBuildHandler(w, r)
	case http.MethodGet:
		getGraphHandler(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func startGraphBuildHandler(w http.ResponseWriter, r *http.Request) {
	req, err := parseGraphBuildRequest(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}

	jobMu.Lock()
	if jobState.Running {
		progress := jobState.Progress
		jobMu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":    "a graph build is already running",
			"progress": progress,
		})
		return
	}

	jobState = graphJobState{
		Running:   true,
		Progress:  0,
		Result:    nil,
		Error:     "",
		Request:   req,
		StartedAt: time.Now(),
	}
	jobMu.Unlock()

	go runGraphBuildJob(req)

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":   "started",
		"progress": 0,
		"request":  req,
	})
}

func checkProgressHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	snapshot := getJobSnapshot()
	status := "idle"
	switch {
	case snapshot.Running:
		status = "running"
	case snapshot.Error != "":
		status = "failed"
	case snapshot.Result != nil:
		status = "completed"
	}

	resp := map[string]any{
		"status":   status,
		"progress": snapshot.Progress,
		"running":  snapshot.Running,
	}
	if snapshot.Error != "" {
		resp["error"] = snapshot.Error
	}
	if snapshot.Request.Address != "" {
		resp["request"] = snapshot.Request
	}
	if !snapshot.StartedAt.IsZero() {
		resp["started_at"] = snapshot.StartedAt
	}
	if !snapshot.FinishedAt.IsZero() {
		resp["finished_at"] = snapshot.FinishedAt
	}

	writeJSON(w, http.StatusOK, resp)
}

func getGraphHandler(w http.ResponseWriter, r *http.Request) {
	snapshot := getJobSnapshot()

	if snapshot.Running {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":    "graph is still being built",
			"progress": snapshot.Progress,
		})
		return
	}

	if snapshot.Error != "" {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error": snapshot.Error,
		})
		return
	}

	if snapshot.Result == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": "no completed graph available. Start one with POST /graph",
		})
		return
	}

	writeJSON(w, http.StatusOK, snapshot.Result)
}

func runGraphBuildJob(req graphBuildRequest) {
	setProgress := func(progress int) {
		jobMu.Lock()
		defer jobMu.Unlock()
		if !jobState.Running {
			return
		}
		if progress < 0 {
			progress = 0
		}
		if progress > 100 {
			progress = 100
		}
		jobState.Progress = progress
	}

	finishFailure := func(err error) {
		jobMu.Lock()
		defer jobMu.Unlock()
		jobState.Running = false
		jobState.Result = nil
		jobState.Error = err.Error()
		jobState.FinishedAt = time.Now()
	}

	finishSuccess := func(graph *TxGraph) {
		jobMu.Lock()
		defer jobMu.Unlock()
		jobState.Running = false
		jobState.Progress = 100
		jobState.Result = graph
		jobState.Error = ""
		jobState.FinishedAt = time.Now()
	}

	ctx := context.Background()

	var (
		graph *TxGraph
		err   error
	)

	switch dataSource {
	case "dune":
		if duneClient == nil {
			finishFailure(errors.New("Dune client is not configured"))
			return
		}
		graph, err = duneClient.BuildGraphFromTokenTransfersWithProgress(
			ctx,
			req.Address,
			req.Depth,
			req.Days,
			func(done, total int) {
				if total <= 0 {
					return
				}
				progress := int(float64(done) / float64(total) * 90.0)
				setProgress(progress)
			},
		)
		if err != nil {
			finishFailure(fmt.Errorf("failed to build graph from Dune: %w", err))
			return
		}
	default:
		setProgress(5)
		builder, buildErr := NewGraphBuilder(rpcURL)
		if buildErr != nil {
			finishFailure(fmt.Errorf("failed to connect to Ethereum RPC: %w", buildErr))
			return
		}

		setProgress(10)
		latest, latestErr := builder.client.BlockNumber(ctx)
		if latestErr != nil {
			finishFailure(fmt.Errorf("failed to get latest block number: %w", latestErr))
			return
		}

		fromBlock := uint64(0)
		if uint64(req.Blocks) < latest {
			fromBlock = latest - uint64(req.Blocks)
		}

		setProgress(15)
		err = builder.ScanBlocksWithProgress(ctx, fromBlock, latest, func(done, total uint64) {
			if total == 0 {
				return
			}
			progress := 15 + int(float64(done)/float64(total)*70.0)
			setProgress(progress)
		})
		if err != nil {
			finishFailure(fmt.Errorf("failed to scan blocks: %w", err))
			return
		}

		setProgress(90)
		graph = builder.BuildGraph(req.Address, req.Depth)
	}

	if graphdbURL != "" && graphdbRepo != "" {
		setProgress(95)
		writer := NewGraphDBWriter(graphdbURL, graphdbRepo)
		if writeErr := writer.WriteGraph(graph); writeErr != nil {
			log.Printf("Warning: GraphDB write failed: %v", writeErr)
		}
	}

	finishSuccess(graph)
}

func parseGraphBuildRequest(r *http.Request) (graphBuildRequest, error) {
	req := graphBuildRequest{
		Depth:  20,
		Blocks: 1000,
		Days:   0,
	}

	if r.ContentLength > 0 {
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&req); err != nil {
			return req, fmt.Errorf("invalid JSON body: %w", err)
		}
	}

	q := r.URL.Query()
	if req.Address == "" {
		req.Address = strings.TrimSpace(q.Get("address"))
	}
	if req.Depth <= 0 {
		if value := strings.TrimSpace(q.Get("depth")); value != "" {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return req, errors.New("invalid 'depth' query parameter")
			}
			req.Depth = parsed
		}
	}
	if req.Blocks <= 0 {
		if value := strings.TrimSpace(q.Get("blocks")); value != "" {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return req, errors.New("invalid 'blocks' query parameter")
			}
			req.Blocks = parsed
		}
	}
	if req.Days < 0 {
		if value := strings.TrimSpace(q.Get("days")); value != "" {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return req, errors.New("invalid 'days' query parameter")
			}
			req.Days = parsed
		}
	}

	req.Address = strings.TrimSpace(req.Address)
	if req.Address == "" {
		return req, errors.New("missing 'address' parameter")
	}
	if req.Depth <= 0 {
		return req, errors.New("'depth' must be > 0")
	}
	if req.Blocks <= 0 {
		return req, errors.New("'blocks' must be > 0")
	}
	if req.Days < 0 {
		return req, errors.New("'days' must be >= 0")
	}

	return req, nil
}

func getJobSnapshot() graphJobState {
	jobMu.RLock()
	defer jobMu.RUnlock()
	return jobState
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("Warning: failed to encode JSON response: %v", err)
	}
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		log.Printf("Warning: invalid value for %s=%q, using %d", name, value, fallback)
		return fallback
	}
	return parsed
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: .env file not found, using environment variables")
	}

	dataSource = strings.ToLower(strings.TrimSpace(os.Getenv("DATA_SOURCE")))
	if dataSource == "" {
		dataSource = "rpc"
	}

	switch dataSource {
	case "dune":
		duneAPIKey := strings.TrimSpace(os.Getenv("DUNE_API_KEY"))
		if duneAPIKey == "" {
			log.Fatal("DUNE_API_KEY environment variable is required when DATA_SOURCE=dune")
		}
		duneClient = NewDuneClient(
			duneAPIKey,
			strings.TrimSpace(os.Getenv("DUNE_PERFORMANCE")),
			strings.TrimSpace(os.Getenv("DUNE_CHAIN")),
			envInt("DUNE_LOOKBACK_DAYS", 30),
			envInt("DUNE_PAGE_LIMIT", 1000),
			envInt("DUNE_HOP_LIMIT", 2000),
			envInt("DUNE_FRONTIER_BATCH", 20),
		)
	default:
		rpcURL = os.Getenv("RPC_URL")
		if rpcURL == "" {
			rpcURL = "https://ethereum-rpc.publicnode.com"
			log.Println("RPC_URL not set, using default:", rpcURL)
		}
	}

	graphdbURL = os.Getenv("GRAPHDB_URL")
	if graphdbURL == "" {
		log.Println("GRAPHDB_URL not set; GraphDB write will be skipped")
	}

	graphdbRepo = os.Getenv("GRAPHDB_REPO")
	if graphdbRepo == "" && graphdbURL != "" {
		graphdbRepo = "wallet-graph"
		log.Println("GRAPHDB_REPO not set, using default:", graphdbRepo)
	}

	if graphdbURL != "" && graphdbRepo != "" {
		graphExplorer = NewGraphExplorer(graphdbURL, graphdbRepo)
	}

	log.Printf("Starting server with DATA_SOURCE=%s, GRAPHDB_URL=%s, GRAPHDB_REPO=%s", dataSource, graphdbURL, graphdbRepo)

	http.HandleFunc("/graph", graphHandler)
	http.HandleFunc("/check-progress", checkProgressHandler)
	http.HandleFunc("/ws/graph", graphWSHandler)

	log.Println("Server listening on :8080")
	log.Println("  POST /graph           -> starts async graph build")
	log.Println("  GET  /check-progress  -> returns completion percentage")
	log.Println("  GET  /graph           -> returns graph when completed")
	log.Println("  GET  /ws/graph        -> websocket incremental graph exploration")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("Server error:", err)
	}
}
