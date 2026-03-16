package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		// Allow local frontend development without CORS/origin friction.
		return true
	},
}

// GraphExplorer queries small wallet neighborhoods from GraphDB.
type GraphExplorer struct {
	baseURL string
	repo    string
	client  *http.Client
}

func NewGraphExplorer(baseURL, repo string) *GraphExplorer {
	return &GraphExplorer{
		baseURL: strings.TrimRight(baseURL, "/"),
		repo:    repo,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

type wsGraphRequest struct {
	Action  string `json:"action"`
	Address string `json:"address"`
	Limit   int    `json:"limit"`
	Offset  int    `json:"offset"`
}

type wsGraphNode struct {
	Address string `json:"address"`
}

type wsGraphResponse struct {
	Type    string        `json:"type"`
	Action  string        `json:"action,omitempty"`
	Address string        `json:"address,omitempty"`
	Nodes   []wsGraphNode `json:"nodes,omitempty"`
	Edges   []*TxEdge     `json:"edges,omitempty"`
	Limit   int           `json:"limit,omitempty"`
	Offset  int           `json:"offset,omitempty"`
	Count   int           `json:"count,omitempty"`
	HasMore bool          `json:"has_more,omitempty"`
	Source  string        `json:"source,omitempty"`
	Error   string        `json:"error,omitempty"`
}

type neighborhoodResult struct {
	nodes   []wsGraphNode
	edges   []*TxEdge
	hasMore bool
	source  string
}

type sparqlSelectResponse struct {
	Results struct {
		Bindings []map[string]sparqlBinding `json:"bindings"`
	} `json:"results"`
}

type sparqlBinding struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func graphWSHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	_ = conn.WriteJSON(wsGraphResponse{
		Type:  "ready",
		Count: 0,
	})

	for {
		var req wsGraphRequest
		if err := conn.ReadJSON(&req); err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return
			}
			log.Printf("websocket read error: %v", err)
			return
		}

		resp := handleWSGraphRequest(r.Context(), req)
		if err := conn.WriteJSON(resp); err != nil {
			log.Printf("websocket write error: %v", err)
			return
		}
	}
}

func handleWSGraphRequest(ctx context.Context, req wsGraphRequest) wsGraphResponse {
	action := strings.ToLower(strings.TrimSpace(req.Action))
	switch action {
	case "seed", "expand":
		// valid actions
	case "ping":
		return wsGraphResponse{Type: "pong"}
	default:
		return wsGraphResponse{
			Type:  "error",
			Error: "invalid action. use 'seed', 'expand', or 'ping'",
		}
	}

	address, err := normalizeAddress(req.Address)
	if err != nil {
		return wsGraphResponse{Type: "error", Error: err.Error()}
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 300 {
		limit = 300
	}

	offset := req.Offset
	if offset < 0 {
		offset = 0
	}

	result, err := fetchNeighborhood(ctx, address, limit, offset)
	if err != nil {
		return wsGraphResponse{Type: "error", Error: err.Error()}
	}

	return wsGraphResponse{
		Type:    "subgraph",
		Action:  action,
		Address: address,
		Nodes:   result.nodes,
		Edges:   result.edges,
		Limit:   limit,
		Offset:  offset,
		Count:   len(result.edges),
		HasMore: result.hasMore,
		Source:  result.source,
	}
}

func fetchNeighborhood(ctx context.Context, address string, limit, offset int) (*neighborhoodResult, error) {
	if graphExplorer != nil {
		nodes, edges, hasMore, err := graphExplorer.FetchNeighborhood(ctx, address, limit, offset)
		if err == nil {
			return &neighborhoodResult{nodes: nodes, edges: edges, hasMore: hasMore, source: "graphdb"}, nil
		}
		log.Printf("GraphDB neighborhood query failed, falling back to memory: %v", err)
	}

	nodes, edges, hasMore, err := fetchNeighborhoodFromInMemoryGraph(address, limit, offset)
	if err != nil {
		return nil, err
	}
	return &neighborhoodResult{nodes: nodes, edges: edges, hasMore: hasMore, source: "memory"}, nil
}

func fetchNeighborhoodFromInMemoryGraph(address string, limit, offset int) ([]wsGraphNode, []*TxEdge, bool, error) {
	snapshot := getJobSnapshot()
	if snapshot.Result == nil {
		return nil, nil, false, fmt.Errorf("no graph data available yet")
	}

	all := make([]*TxEdge, 0)
	for _, edge := range snapshot.Result.Edges {
		if strings.EqualFold(edge.From, address) || strings.EqualFold(edge.To, address) {
			all = append(all, edge)
		}
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].BlockNumber == all[j].BlockNumber {
			return all[i].Hash > all[j].Hash
		}
		return all[i].BlockNumber > all[j].BlockNumber
	})

	if offset >= len(all) {
		return []wsGraphNode{{Address: address}}, []*TxEdge{}, false, nil
	}

	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	page := all[offset:end]
	hasMore := end < len(all)

	nodeSet := map[string]bool{address: true}
	for _, edge := range page {
		nodeSet[strings.ToLower(edge.From)] = true
		nodeSet[strings.ToLower(edge.To)] = true
	}

	nodes := make([]wsGraphNode, 0, len(nodeSet))
	for addr := range nodeSet {
		nodes = append(nodes, wsGraphNode{Address: addr})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Address < nodes[j].Address })

	return nodes, page, hasMore, nil
}

func (e *GraphExplorer) FetchNeighborhood(ctx context.Context, address string, limit, offset int) ([]wsGraphNode, []*TxEdge, bool, error) {
	query := buildNeighborhoodSPARQL(address, limit, offset)
	rows, err := e.runSelect(ctx, query)
	if err != nil {
		return nil, nil, false, err
	}

	edges := make([]*TxEdge, 0, len(rows))
	nodeSet := map[string]bool{address: true}

	for _, row := range rows {
		edge := &TxEdge{
			Hash:        getBindingValue(row, "txHash"),
			From:        strings.ToLower(getBindingValue(row, "fromAddr")),
			To:          strings.ToLower(getBindingValue(row, "toAddr")),
			ValueETH:    getBindingValue(row, "valueETH"),
			BlockNumber: parseUint64(getBindingValue(row, "blockNumber")),
		}
		if edge.Hash == "" || edge.From == "" || edge.To == "" {
			continue
		}
		edges = append(edges, edge)
		nodeSet[edge.From] = true
		nodeSet[edge.To] = true
	}

	nodes := make([]wsGraphNode, 0, len(nodeSet))
	for addr := range nodeSet {
		nodes = append(nodes, wsGraphNode{Address: addr})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Address < nodes[j].Address })

	hasMore := len(rows) == limit
	return nodes, edges, hasMore, nil
}

func (e *GraphExplorer) runSelect(ctx context.Context, query string) ([]map[string]sparqlBinding, error) {
	selectURL := fmt.Sprintf("%s/repositories/%s", e.baseURL, e.repo)
	form := url.Values{}
	form.Set("query", query)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, selectURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to build GraphDB query request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/sparql-results+json")

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GraphDB query failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("GraphDB query returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	var parsed sparqlSelectResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("failed to decode GraphDB response: %w", err)
	}

	return parsed.Results.Bindings, nil
}

func buildNeighborhoodSPARQL(address string, limit, offset int) string {
	return fmt.Sprintf(`PREFIX wg: <urn:wallet-graph:>
SELECT ?txHash ?fromAddr ?toAddr ?valueETH ?blockNumber
WHERE {
	?tx a wg:Transaction ;
			wg:hash ?txHash ;
			wg:from ?fromWallet ;
			wg:to ?toWallet ;
			wg:valueETH ?valueETH ;
			wg:blockNumber ?blockNumber .
	?fromWallet wg:address ?fromAddr .
	?toWallet wg:address ?toAddr .
	FILTER(lcase(str(?fromAddr)) = "%s" || lcase(str(?toAddr)) = "%s")
}
ORDER BY DESC(?blockNumber)
LIMIT %d
OFFSET %d`, address, address, limit, offset)
}

func getBindingValue(binding map[string]sparqlBinding, key string) string {
	value, ok := binding[key]
	if !ok {
		return ""
	}
	return strings.TrimSpace(value.Value)
}

func parseUint64(value string) uint64 {
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}
