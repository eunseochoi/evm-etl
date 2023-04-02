package node

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/datadaodevs/evm-etl/protos/go/protos/evm/raw"
	"github.com/datadaodevs/evm-etl/shared/util"
	framework "github.com/datadaodevs/go-service-framework/util"
	"github.com/ethereum/go-ethereum/ethclient"
	"google.golang.org/protobuf/encoding/protojson"
	"net/http"
	"strings"
	"time"
)

// Client is a generic node client interface
type Client interface {
	EthBlockNumber(ctx context.Context) (uint64, error)
	EthGetBlockByNumber(ctx context.Context, blockNumber uint64) (*raw.Block, error)
	DebugTraceBlock(ctx context.Context, blockNumber uint64) ([]*raw.CallTrace, error)
	GetBlockReceipt(ctx context.Context, blockNumber uint64) ([]*raw.TransactionReceipt, error)
	GetTransactionReceipt(ctx context.Context, txHash string) (*raw.TransactionReceipt, error)
}

// client is an ethclient-based implementation
type client struct {
	url          string
	parsedClient *ethclient.Client
	httpClient   *http.Client
	config       *Config
}

// jrpcBlockResult is a raw node client result for a block
type jrpcBlockResult struct {
	Jsonrpc string          `json:"jsonrpc"`
	Id      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   interface{}     `json:"error"`
}

// jrpcTraceResult is a raw node client result for getting traces
type jrpcTraceResult struct {
	Jsonrpc string      `json:"jsonrpc"`
	Id      int         `json:"id"`
	Result  []Trace     `json:"result"`
	Error   interface{} `json:"error"`
}

// Trace is a single trace object
type Trace struct {
	Result json.RawMessage `json:"result"`
	Error  interface{}     `json:"error"`
}

// jrpcBlockReceiptsResult is a raw block receipts result from a node client
type jrpcBlockReceiptsResult struct {
	Jsonrpc string            `json:"jsonrpc"`
	Id      int               `json:"id"`
	Result  []json.RawMessage `json:"result"`
	Error   interface{}       `json:"error"`
}

// jrpcTxReceiptResult is a raw tx receipts result from a node client
type jrpcTxReceiptResult struct {
	Jsonrpc string          `json:"jsonrpc"`
	Id      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   interface{}     `json:"error"`
}

// NewClient instantiates a new client
func NewClient(cfg *Config, logger framework.Logger) (*client, error) {
	parsedClient, err := ethclient.Dial(cfg.NodeHost)
	if err != nil {
		logger.Fatal(err)
		return nil, err
	}

	httpClient := &http.Client{
		Timeout: time.Second * 300,
	}

	return &client{
		url:          cfg.NodeHost,
		httpClient:   httpClient,
		parsedClient: parsedClient,
		config:       cfg,
	}, nil
}

// MustNewClient instantiates a new client, with fatal exit on error
func MustNewClient(config *Config, logger framework.Logger) *client {
	client, err := NewClient(config, logger)
	if err != nil {
		logger.Fatal("Failed to instantiate node client")
	}
	return client
}

// EthBlockNumber gets the most recent block number
func (c *client) EthBlockNumber(ctx context.Context) (uint64, error) {
	number, err := c.parsedClient.BlockNumber(ctx)
	if err != nil {
		return 0, err
	}
	return number, nil
}

// EthGetBlockByNumber gets a block by number
func (c *client) EthGetBlockByNumber(ctx context.Context, blockNumber uint64) (*raw.Block, error) {
	stringPayload := fmt.Sprintf("{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"%s\", true]}", util.BlockNumberToHex(blockNumber))
	var res jrpcBlockResult
	if err := c.do(ctx, stringPayload, &res); err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, fmt.Errorf("%v", res.Error)
	}
	data := &raw.Block{}
	if err := protojson.Unmarshal(res.Result, data); err != nil {
		return nil, err
	}

	return data, nil
}

func (c *client) DebugTraceBlock(ctx context.Context, blockNumber uint64) ([]*raw.CallTrace, error) {
	// genesis block has no traces
	if blockNumber == 0 {
		return nil, nil
	}

	stringPayload := fmt.Sprintf("{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"debug_traceBlockByNumber\",\"params\":[\"%s\",{\"tracer\": \"callTracer\", \"timeout\":\"300s\"}]}", util.BlockNumberToHex(blockNumber))
	var res jrpcTraceResult
	if err := c.do(ctx, stringPayload, &res); err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, fmt.Errorf("%v", res.Error)
	}

	var rawTraces []*raw.CallTrace
	for _, trace := range res.Result {
		if trace.Error != nil {
			return nil, fmt.Errorf("%v", trace.Error)
		}
		rawTrace := &raw.CallTrace{}
		if err := protojson.Unmarshal(trace.Result, rawTrace); err != nil {
			return nil, err
		}
		rawTraces = append(rawTraces, rawTrace)
	}

	return rawTraces, nil
}

func (c *client) GetBlockReceipt(ctx context.Context, blockNumber uint64) ([]*raw.TransactionReceipt, error) {
	stringPayload := fmt.Sprintf("{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockReceipts\",\"params\":[\"%s\"]}", util.BlockNumberToHex(blockNumber))

	var res jrpcBlockReceiptsResult
	if err := c.do(ctx, stringPayload, &res); err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, fmt.Errorf("%v", res.Error)
	}

	var rawReceipts []*raw.TransactionReceipt
	for _, receipt := range res.Result {
		rawReceipt := &raw.TransactionReceipt{}
		if err := protojson.Unmarshal(receipt, rawReceipt); err != nil {
			return nil, err
		}
		rawReceipts = append(rawReceipts, rawReceipt)
	}

	return rawReceipts, nil
}

func (c *client) GetTransactionReceipt(ctx context.Context, txHash string) (*raw.TransactionReceipt, error) {
	stringPayload := fmt.Sprintf("{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_getTransactionReceipt\",\"params\":[\"%s\"]}", txHash)
	var res jrpcTxReceiptResult
	if err := c.do(ctx, stringPayload, &res); err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, fmt.Errorf("%v", res.Error)
	}

	rawReceipt := &raw.TransactionReceipt{}
	if err := protojson.Unmarshal(res.Result, rawReceipt); err != nil {
		return nil, err
	}

	return rawReceipt, nil
}

// do makes a generic HTTP request to the given node server
func (c *client) do(ctx context.Context, strPayload string, respObj interface{}) error {
	client := http.Client{}
	reqPayload := strings.NewReader(strPayload)
	req, err := http.NewRequest(http.MethodPost, c.url, reqPayload)
	if err != nil {
		return err
	}
	req.Header.Add("accept", "application/json")
	req.Header.Add("content-type", "application/json")

	ctx, cancel := context.WithTimeout(ctx, c.config.RPCTimeout)
	defer cancel()
	req = req.WithContext(ctx)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("Received non-200 response from server: [status:%d]", resp.StatusCode)
	}

	if respObj != nil {
		return json.NewDecoder(resp.Body).Decode(respObj)
	}
	return nil
}
