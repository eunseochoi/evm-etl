package node

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coherentopensource/evm-etl/shared/util"
	framework "github.com/coherentopensource/go-service-framework/util"
	"github.com/ethereum/go-ethereum/ethclient"
	"net/http"
	"strings"
)

type Client interface {
	GetLatestBlockNumber(ctx context.Context) (uint64, error)
	GetBlockByNumber(ctx context.Context, blockNumber uint64) (*BlockResponse, error)
	GetTracesForBlock(ctx context.Context, blockNumber uint64) (*TraceResponse, error)
	GetBlockReceipt(ctx context.Context, blockNumber uint64) (*BlockReceiptResponse, error)
	GetTransactionReceipt(ctx context.Context, txHash string) (*TxReceiptResponse, error)
}

// client is an ethclient-based implementation
type client struct {
	url          string
	parsedClient *ethclient.Client
	httpClient   *http.Client
	config       *Config
}

// BlockResponse is a raw node client result for a block
type BlockResponse struct {
	Jsonrpc string          `json:"jsonrpc"`
	Id      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   interface{}     `json:"error"`
}

// TraceResponse is a raw node client result for getting traces
type TraceResponse struct {
	Jsonrpc string        `json:"jsonrpc"`
	Id      int           `json:"id"`
	Result  []TraceResult `json:"result"`
	Error   interface{}   `json:"error"`
}

// TraceResult is a single trace object
type TraceResult struct {
	Result json.RawMessage `json:"result"`
	Error  interface{}     `json:"error"`
}

// BlockReceiptResponse is a raw block receipts result from a node client
type BlockReceiptResponse struct {
	Jsonrpc string            `json:"jsonrpc"`
	Id      int               `json:"id"`
	Result  []json.RawMessage `json:"result"`
	Error   interface{}       `json:"error"`
}

// TxReceiptResponse is a raw tx receipts result from a node client
type TxReceiptResponse struct {
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
		Timeout: cfg.RPCTimeout,
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
func (c *client) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	number, err := c.parsedClient.BlockNumber(ctx)
	if err != nil {
		return 0, err
	}
	return number, nil
}

// EthGetBlockByNumber gets a block by number
func (c *client) GetBlockByNumber(ctx context.Context, blockNumber uint64) (*BlockResponse, error) {
	stringPayload := fmt.Sprintf("{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"%s\", true]}", util.BlockNumberToHex(blockNumber))
	var res BlockResponse
	if err := c.do(ctx, stringPayload, &res); err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, fmt.Errorf("%v", res.Error)
	}

	return &res, nil
}

func (c *client) GetTracesForBlock(ctx context.Context, blockNumber uint64) (*TraceResponse, error) {
	// genesis block has no traces
	if blockNumber == 0 {
		return nil, nil
	}

	stringPayload := fmt.Sprintf("{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"debug_traceBlockByNumber\",\"params\":[\"%s\",{\"tracer\": \"callTracer\", \"timeout\":\"300s\"}]}", util.BlockNumberToHex(blockNumber))
	var res TraceResponse
	if err := c.do(ctx, stringPayload, &res); err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, fmt.Errorf("%v", res.Error)
	}

	return &res, nil
}

func (c *client) GetBlockReceipt(ctx context.Context, blockNumber uint64) (*BlockReceiptResponse, error) {
	stringPayload := fmt.Sprintf("{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockReceipts\",\"params\":[\"%s\"]}", util.BlockNumberToHex(blockNumber))

	var res BlockReceiptResponse
	if err := c.do(ctx, stringPayload, &res); err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, fmt.Errorf("%v", res.Error)
	}

	return &res, nil
}

func (c *client) GetTransactionReceipt(ctx context.Context, txHash string) (*TxReceiptResponse, error) {
	stringPayload := fmt.Sprintf("{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_getTransactionReceipt\",\"params\":[\"%s\"]}", txHash)
	var res TxReceiptResponse
	if err := c.do(ctx, stringPayload, &res); err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, fmt.Errorf("%v", res.Error)
	}

	return &res, nil
}

// do makes a generic HTTP request to the given node server
func (c *client) do(ctx context.Context, strPayload string, respObj interface{}) error {
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
	resp, err := c.httpClient.Do(req)
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
