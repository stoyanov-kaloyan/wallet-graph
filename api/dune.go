package main

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/duneanalytics/duneapi-client-go/config"
	dunesdk "github.com/duneanalytics/duneapi-client-go/dune"
	"github.com/duneanalytics/duneapi-client-go/models"
)

var (
	hexAddressPattern = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)
	chainPattern      = regexp.MustCompile(`^[a-z0-9_]+$`)
)

// DuneClient wraps the official Dune SDK with graph-building config.
type DuneClient struct {
	sdk           dunesdk.DuneClient
	performance   string
	chain         string
	lookbackDays  int
	pageLimit     uint32
	hopLimit      int
	frontierBatch int
}

func NewDuneClient(apiKey, performance, chain string, lookbackDays, pageLimit, hopLimit, frontierBatch int) *DuneClient {
	if performance != "large" {
		performance = "medium"
	}

	chain = strings.ToLower(strings.TrimSpace(chain))
	if chain == "" || !chainPattern.MatchString(chain) {
		chain = "ethereum"
	}

	if lookbackDays <= 0 {
		lookbackDays = 30
	}
	if pageLimit <= 0 {
		pageLimit = 1000
	}
	if hopLimit <= 0 {
		hopLimit = 2000
	}
	if frontierBatch <= 0 {
		frontierBatch = 20
	}

	return &DuneClient{
		sdk:           dunesdk.NewDuneClient(config.FromAPIKey(apiKey)),
		performance:   performance,
		chain:         chain,
		lookbackDays:  lookbackDays,
		pageLimit:     uint32(pageLimit),
		hopLimit:      hopLimit,
		frontierBatch: frontierBatch,
	}
}

// BuildGraphFromTokenTransfers expands from seedAddr via BFS using Dune tokens.transfers.
func (c *DuneClient) BuildGraphFromTokenTransfers(ctx context.Context, seedAddr string, maxDepth, daysOverride int) (*TxGraph, error) {
	return c.BuildGraphFromTokenTransfersWithProgress(ctx, seedAddr, maxDepth, daysOverride, nil)
}

// BuildGraphFromTokenTransfersWithProgress behaves like BuildGraphFromTokenTransfers and reports depth progress.
func (c *DuneClient) BuildGraphFromTokenTransfersWithProgress(
	ctx context.Context,
	seedAddr string,
	maxDepth, daysOverride int,
	onProgress func(done, total int),
) (*TxGraph, error) {
	seed, err := normalizeAddress(seedAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid seed address: %w", err)
	}

	if maxDepth <= 0 {
		maxDepth = 1
	}

	lookbackDays := c.lookbackDays
	if daysOverride > 0 {
		lookbackDays = daysOverride
	}

	graph := &TxGraph{Nodes: make(map[string]*WalletNode)}
	graph.Nodes[seed] = &WalletNode{Address: seed, Depth: 0}

	visited := map[string]bool{seed: true}
	seenEdge := make(map[string]bool)
	currentLevel := []string{seed}

	if onProgress != nil {
		onProgress(0, maxDepth)
	}

	for depth := 0; depth < maxDepth && len(currentLevel) > 0; depth++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		nextLevel := make([]string, 0)

		for i := 0; i < len(currentLevel); i += c.frontierBatch {
			end := i + c.frontierBatch
			if end > len(currentLevel) {
				end = len(currentLevel)
			}
			batch := currentLevel[i:end]

			edges, err := c.fetchTokenTransfersForAddresses(ctx, batch, lookbackDays, c.hopLimit)
			if err != nil {
				return nil, fmt.Errorf("depth %d batch %d-%d: %w", depth, i+1, end, err)
			}

			for _, edge := range edges {
				key := makeEdgeKey(edge)
				if !seenEdge[key] {
					seenEdge[key] = true
					graph.Edges = append(graph.Edges, edge)
				}
				for _, addr := range []string{edge.From, edge.To} {
					if !visited[addr] {
						visited[addr] = true
						graph.Nodes[addr] = &WalletNode{Address: addr, Depth: depth + 1}
						nextLevel = append(nextLevel, addr)
					}
				}
			}
		}

		if onProgress != nil {
			onProgress(depth+1, maxDepth)
		}

		log.Printf("Dune BFS depth=%d: next=%d nodes=%d edges=%d",
			depth, len(nextLevel), len(graph.Nodes), len(graph.Edges))
		currentLevel = nextLevel
	}

	if onProgress != nil {
		onProgress(maxDepth, maxDepth)
	}

	return graph, nil
}

func (c *DuneClient) fetchTokenTransfersForAddresses(ctx context.Context, addresses []string, lookbackDays, limit int) ([]*TxEdge, error) {
	sql, err := c.buildTokenTransfersSQL(addresses, lookbackDays, limit)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	execution, err := c.sdk.RunSQL(models.ExecuteSQLRequest{
		SQL:         sql,
		Performance: c.performance,
	})
	if err != nil {
		return nil, fmt.Errorf("dune RunSQL: %w", err)
	}

	// WaitGetResults polls internally until the execution finishes.
	firstPage, err := execution.WaitGetResults(5*time.Second, 10)
	if err != nil {
		return nil, fmt.Errorf("dune WaitGetResults: %w", err)
	}
	if firstPage.Error != nil {
		return nil, fmt.Errorf("dune execution error: %s", firstPage.Error.Message)
	}

	rows := make([]map[string]any, 0, firstPage.Result.Metadata.TotalRowCount)
	rows = append(rows, firstPage.Result.Rows...)

	// Paginate remaining pages when the result set is larger than first page.
	nextOffset := firstPage.NextOffset
	for nextOffset != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		page, err := c.sdk.QueryResultsV2(execution.GetID(), models.ResultOptions{
			Page: &models.ResultPageOption{
				Offset: *nextOffset,
				Limit:  c.pageLimit,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("dune paginate offset=%d: %w", *nextOffset, err)
		}
		if page.Error != nil {
			return nil, fmt.Errorf("dune paginate error: %s", page.Error.Message)
		}
		if len(page.Result.Rows) == 0 {
			break
		}
		rows = append(rows, page.Result.Rows...)
		nextOffset = page.NextOffset
	}

	edges := make([]*TxEdge, 0, len(rows))
	for _, row := range rows {
		if edge, ok := rowToTxEdge(row); ok {
			edges = append(edges, edge)
		}
	}
	return edges, nil
}

func (c *DuneClient) buildTokenTransfersSQL(addresses []string, lookbackDays, limit int) (string, error) {
	if len(addresses) == 0 {
		return "", fmt.Errorf("no addresses provided")
	}

	if lookbackDays <= 0 {
		lookbackDays = c.lookbackDays
	}
	if limit <= 0 {
		limit = c.hopLimit
	}

	inVals := make([]string, 0, len(addresses))
	for _, addr := range addresses {
		normalized, err := normalizeAddress(addr)
		if err != nil {
			return "", err
		}
		inVals = append(inVals, fmt.Sprintf("from_hex('%s')", strings.TrimPrefix(normalized, "0x")))
	}
	inClause := strings.Join(inVals, ",")

	return fmt.Sprintf(`SELECT
  concat('0x', lower(to_hex(tx_hash))) AS tx_hash,
  concat('0x', lower(to_hex("from"))) AS from_address,
  concat('0x', lower(to_hex("to"))) AS to_address,
  cast(amount as varchar) AS amount,
  cast(block_number as bigint) AS block_number
FROM tokens.transfers
WHERE blockchain = '%s'
  AND block_time >= now() - interval '%d' day
  AND (
    "from" IN (%s)
    OR "to" IN (%s)
  )
ORDER BY block_time DESC
LIMIT %d`, c.chain, lookbackDays, inClause, inClause, limit), nil
}

func rowToTxEdge(row map[string]any) (*TxEdge, bool) {
	from, err := normalizeAddress(anyToString(row["from_address"]))
	if err != nil {
		return nil, false
	}
	to, err := normalizeAddress(anyToString(row["to_address"]))
	if err != nil {
		return nil, false
	}

	txHash := strings.ToLower(strings.TrimSpace(anyToString(row["tx_hash"])))
	if txHash != "" && !strings.HasPrefix(txHash, "0x") {
		txHash = "0x" + txHash
	}
	if txHash == "" {
		txHash = "dune"
	}

	amount := strings.TrimSpace(anyToString(row["amount"]))
	if amount == "" {
		amount = "0"
	}

	return &TxEdge{
		Hash:        txHash,
		From:        from,
		To:          to,
		ValueETH:    amount,
		BlockNumber: anyToUint64(row["block_number"]),
	}, true
}

func makeEdgeKey(edge *TxEdge) string {
	return edge.Hash + "|" + edge.From + "|" + edge.To + "|" + edge.ValueETH + "|" + strconv.FormatUint(edge.BlockNumber, 10)
}

func normalizeAddress(address string) (string, error) {
	addr := strings.ToLower(strings.TrimSpace(address))
	if !strings.HasPrefix(addr, "0x") {
		addr = "0x" + addr
	}
	if !hexAddressPattern.MatchString(addr) {
		return "", fmt.Errorf("invalid address %q", address)
	}
	return addr, nil
}

func anyToString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int64:
		return strconv.FormatInt(v, 10)
	default:
		return fmt.Sprint(v)
	}
}

func anyToUint64(value any) uint64 {
	switch v := value.(type) {
	case nil:
		return 0
	case float64:
		if v < 0 {
			return 0
		}
		return uint64(v)
	case int64:
		if v < 0 {
			return 0
		}
		return uint64(v)
	case string:
		parsed, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}
