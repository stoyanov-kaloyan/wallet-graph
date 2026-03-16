package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

const sparqlPrefixes = `PREFIX wg: <urn:wallet-graph:>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
`

// GraphDBWriter sends batched SPARQL INSERT DATA requests to a GraphDB repo.
type GraphDBWriter struct {
	baseURL   string // e.g. "http://localhost:7200"
	repo      string // repository name
	batchSize int    // triples per request
}

func NewGraphDBWriter(baseURL, repo string) *GraphDBWriter {
	return &GraphDBWriter{
		baseURL:   strings.TrimRight(baseURL, "/"),
		repo:      repo,
		batchSize: 500,
	}
}

// TxEdge represents a single transaction between two wallets.
type TxEdge struct {
	Hash        string `json:"hash"`
	From        string `json:"from"`
	To          string `json:"to"`
	ValueETH    string `json:"value_eth"`
	BlockNumber uint64 `json:"block"`
}

// WalletNode is a vertex in the transaction graph.
type WalletNode struct {
	Address string `json:"address"`
	Depth   int    `json:"depth"`
}

// TxGraph is the resulting directed transaction graph.
type TxGraph struct {
	Nodes map[string]*WalletNode `json:"nodes"`
	Edges []*TxEdge              `json:"edges"`
}

func formatEth(wei *big.Int) string {
	if wei == nil || wei.Sign() == 0 {
		return "0"
	}
	f, _ := new(big.Float).Quo(
		new(big.Float).SetInt(wei),
		big.NewFloat(1e18),
	).Float64()
	return fmt.Sprintf("%.6f", f)
}

// GraphBuilder scans blocks and builds address-indexed transaction data.
type GraphBuilder struct {
	client    *ethclient.Client
	chainID   *big.Int
	addrEdges map[string][]*TxEdge // address (lower) -> all edges where addr is from or to
}

func NewGraphBuilder(rpcURL string) (*GraphBuilder, error) {
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	chainID, err := client.ChainID(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %w", err)
	}
	return &GraphBuilder{
		client:    client,
		chainID:   chainID,
		addrEdges: make(map[string][]*TxEdge),
	}, nil
}

// WriteGraph serialises all nodes and edges as RDF triples and pushes them
// to GraphDB in batches.
func (w *GraphDBWriter) WriteGraph(graph *TxGraph) error {
	var triples []string

	for addr, node := range graph.Nodes {
		t := fmt.Sprintf(
			`  %s a wg:Wallet ; wg:address "%s" ; wg:depth %d .`,
			walletURI(addr), addr, node.Depth,
		)
		triples = append(triples, t)
	}

	for _, edge := range graph.Edges {
		// Only emit edge triples whose endpoints are in the node set.
		if _, ok := graph.Nodes[edge.From]; !ok {
			continue
		}
		if _, ok := graph.Nodes[edge.To]; !ok {
			continue
		}
		t := fmt.Sprintf(
			`  %s a wg:Transaction ; wg:hash "%s" ; wg:from %s ; wg:to %s ; wg:valueETH "%s"^^xsd:decimal ; wg:blockNumber %d .`,
			txURI(transactionNodeID(edge)), edge.Hash,
			walletURI(edge.From), walletURI(edge.To),
			edge.ValueETH, edge.BlockNumber,
		)
		triples = append(triples, t)
	}

	total := len(triples)
	log.Printf("GraphDB: writing %d triples to %s/repositories/%s", total, w.baseURL, w.repo)

	for i := 0; i < total; i += w.batchSize {
		end := i + w.batchSize
		if end > total {
			end = total
		}
		update := sparqlPrefixes + "INSERT DATA {\n" +
			strings.Join(triples[i:end], "\n") +
			"\n}"
		if err := w.sparqlUpdate(update); err != nil {
			return fmt.Errorf("batch %d-%d: %w", i+1, end, err)
		}
		log.Printf("GraphDB: inserted triples %d–%d / %d", i+1, end, total)
	}
	return nil
}

func transactionNodeID(edge *TxEdge) string {
	hash := strings.TrimSpace(edge.Hash)
	if hash == "" {
		hash = "unknown"
	}
	return hash + "_" + addressSuffix(edge.From) + "_" + addressSuffix(edge.To) + "_" + strconv.FormatUint(edge.BlockNumber, 10)
}

func addressSuffix(addr string) string {
	if len(addr) >= 10 {
		return addr[2:10]
	}
	return strings.ReplaceAll(addr, "0x", "")
}

// ScanBlocks fetches every block in [fromBlock, toBlock] and indexes all
// native-ETH transactions by the addresses involved.
func (gb *GraphBuilder) ScanBlocks(ctx context.Context, fromBlock, toBlock uint64) error {
	return gb.ScanBlocksWithProgress(ctx, fromBlock, toBlock, nil)
}

// ScanBlocksWithProgress behaves like ScanBlocks and reports completion via callback.
func (gb *GraphBuilder) ScanBlocksWithProgress(ctx context.Context, fromBlock, toBlock uint64, onProgress func(done, total uint64)) error {
	if toBlock < fromBlock {
		return nil
	}

	signer := types.LatestSignerForChainID(gb.chainID)
	total := toBlock - fromBlock + 1
	log.Printf("Scanning %d blocks (%d → %d)...", total, fromBlock, toBlock)
	if onProgress != nil {
		onProgress(0, total)
	}

	for i := fromBlock; i <= toBlock; i++ {
		block, err := gb.client.BlockByNumber(ctx, big.NewInt(int64(i)))
		if err != nil {
			log.Printf("Warning: skipping block %d: %v", i, err)
			continue
		}

		for _, tx := range block.Transactions() {
			if tx.To() == nil {
				continue // skip contract deployments
			}
			from, err := types.Sender(signer, tx)
			if err != nil {
				continue
			}

			edge := &TxEdge{
				Hash:        tx.Hash().Hex(),
				From:        strings.ToLower(from.Hex()),
				To:          strings.ToLower(tx.To().Hex()),
				ValueETH:    formatEth(tx.Value()),
				BlockNumber: block.NumberU64(),
			}

			gb.addrEdges[edge.From] = append(gb.addrEdges[edge.From], edge)
			if edge.To != edge.From {
				gb.addrEdges[edge.To] = append(gb.addrEdges[edge.To], edge)
			}
		}

		done := i - fromBlock + 1
		if onProgress != nil {
			onProgress(done, total)
		}
		if done%100 == 0 || done == total {
			log.Printf("  %d/%d blocks scanned (%d unique addresses so far)",
				done, total, len(gb.addrEdges))
		}
	}

	log.Printf("Scan complete: %d unique addresses indexed.", len(gb.addrEdges))
	return nil
}

// BuildGraph performs a BFS from seedAddr, expanding up to maxDepth hops
// using only transactions found in the locally scanned blocks.
func (gb *GraphBuilder) BuildGraph(seedAddr string, maxDepth int) *TxGraph {
	seedAddr = strings.ToLower(seedAddr)

	graph := &TxGraph{
		Nodes: make(map[string]*WalletNode),
	}

	type bfsItem struct {
		addr  string
		depth int
	}

	visited := make(map[string]bool)
	seenEdge := make(map[string]bool)
	queue := []bfsItem{{addr: seedAddr, depth: 0}}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		if visited[item.addr] {
			continue
		}
		visited[item.addr] = true

		graph.Nodes[item.addr] = &WalletNode{
			Address: item.addr,
			Depth:   item.depth,
		}

		if item.depth >= maxDepth {
			continue
		}

		for _, edge := range gb.addrEdges[item.addr] {
			if seenEdge[edge.Hash] {
				continue
			}
			seenEdge[edge.Hash] = true
			graph.Edges = append(graph.Edges, edge)

			// Enqueue the counterparty
			next := edge.To
			if edge.To == item.addr {
				next = edge.From
			}
			if !visited[next] {
				queue = append(queue, bfsItem{addr: next, depth: item.depth + 1})
			}
		}
	}

	return graph
}

// ToDOT renders the graph in Graphviz DOT format.
func (g *TxGraph) ToDOT() string {
	var sb strings.Builder
	sb.WriteString("digraph wallet_graph {\n")
	sb.WriteString("  node [shape=ellipse fontname=monospace];\n")

	for addr, node := range g.Nodes {
		label := addr[:8] + "…" + addr[len(addr)-4:]
		color := "lightblue"
		if node.Depth == 0 {
			color = "gold"
		}
		sb.WriteString(fmt.Sprintf(
			"  %q [label=%q fillcolor=%s style=filled depth=%d];\n",
			addr, label, color, node.Depth,
		))
	}

	for _, edge := range g.Edges {
		if _, ok := g.Nodes[edge.From]; !ok {
			continue
		}
		if _, ok := g.Nodes[edge.To]; !ok {
			continue
		}
		sb.WriteString(fmt.Sprintf(
			"  %q -> %q [label=%q];\n",
			edge.From, edge.To, edge.ValueETH+" ETH",
		))
	}

	sb.WriteString("}\n")
	return sb.String()
}

// ClearGraph issues a SPARQL DROP SILENT DEFAULT to wipe the default graph.
func (w *GraphDBWriter) ClearGraph() error {
	return w.sparqlUpdate("DROP SILENT DEFAULT")
}

func (w *GraphDBWriter) sparqlUpdate(update string) error {
	url := fmt.Sprintf("%s/repositories/%s/statements", w.baseURL, w.repo)
	resp, err := http.Post(url, "application/sparql-update", strings.NewReader(update)) //nolint:noctx
	if err != nil {
		return fmt.Errorf("HTTP POST to GraphDB failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("GraphDB returned HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
