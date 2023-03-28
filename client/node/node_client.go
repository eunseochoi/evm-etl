package node

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/datadaodevs/evm-etl/protos/go/protos/evm/raw"
	"github.com/datadaodevs/go-service-framework/constants"
	"github.com/datadaodevs/go-service-framework/util"
	"github.com/ethereum/go-ethereum/ethclient"
	"google.golang.org/protobuf/encoding/protojson"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type client struct {
	url          string
	parsedClient *ethclient.Client
	httpClient   *http.Client
	config       *Config
}

type jrpcBlockResult struct {
	Jsonrpc string          `json:"jsonrpc"`
	Id      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   interface{}     `json:"error"`
}

type jrpcTraceResult struct {
	Jsonrpc string      `json:"jsonrpc"`
	Id      int         `json:"id"`
	Result  []Trace     `json:"result"`
	Error   interface{} `json:"error"`
}

type Trace struct {
	Result json.RawMessage `json:"result"`
	Error  interface{}     `json:"error"`
}

type jrpcBlockReceiptsResult struct {
	Jsonrpc string            `json:"jsonrpc"`
	Id      int               `json:"id"`
	Result  []json.RawMessage `json:"result"`
	Error   interface{}       `json:"error"`
}

type jrpcTxReceiptResult struct {
	Jsonrpc string          `json:"jsonrpc"`
	Id      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   interface{}     `json:"error"`
}

func getNode(config *Config, blockchain constants.Blockchain) string {
	switch blockchain {
	case constants.Ethereum:
		return config.EthNodeRPC
	case constants.Optimism:
		return config.OptNodeRPC
	case constants.Polygon:
		return config.PolyNodeRPC
	case constants.Goerli:
		return config.GoerliNodeRPC
	}
	return ""
}

func NewClient(config *Config, logger util.Logger) (*client, error) {
	url := getNode(config, config.Blockchain)
	parsedClient, err := ethclient.Dial(url)
	if err != nil {
		logger.Fatal(err)
		return nil, err
	}

	httpClient := &http.Client{
		Timeout: time.Second * 300,
	}

	return &client{
		url:          url,
		httpClient:   httpClient,
		parsedClient: parsedClient,
		config:       config,
	}, nil
}

func MustNewClient(config *Config, logger util.Logger) *client {
	client, err := NewClient(config, logger)
	if err != nil {
		logger.Fatal("Failed to instantiate node client")
	}
	return client
}

type Client interface {
	EthBlockNumber(ctx context.Context) (uint64, error)
	EthGetBlockByNumber(blockNumber uint64) (*raw.Block, error)
	DebugTraceBlock(blockNumber uint64) ([]*raw.CallTrace, error)
	GetBlockReceipt(blockNumber uint64) ([]*raw.TransactionReceipt, error)
	GetTransactionReceipt(txHash string) (*raw.TransactionReceipt, error)
}

func (c *client) EthBlockNumber(ctx context.Context) (uint64, error) {
	number, err := c.parsedClient.BlockNumber(ctx)
	if err != nil {
		return 0, err
	}
	return number, nil
}

func (c *client) EthGetBlockByNumber(blockNumber uint64) (*raw.Block, error) {
	hexBlockNumber := "0x" + fmt.Sprintf("%x", blockNumber)
	stringPayload := fmt.Sprintf("{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"%s\", true]}", hexBlockNumber)
	reqPayload := strings.NewReader(stringPayload)
	req, err := http.NewRequest("POST", c.url, reqPayload)
	if err != nil {
		return nil, err
	}

	req.Header.Add("accept", "application/json")
	req.Header.Add("content-type", "application/json")
	ctx, cancel := context.WithTimeout(req.Context(), 10*time.Second)
	defer cancel()

	req = req.WithContext(ctx)
	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	jrpcResult := &jrpcBlockResult{}
	err = json.Unmarshal(body, jrpcResult)
	if err != nil {
		return nil, err
	}
	if jrpcResult.Error != nil {
		return nil, fmt.Errorf("%v", jrpcResult.Error)
	}

	data := &raw.Block{}
	err = protojson.Unmarshal(jrpcResult.Result, data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (c *client) DebugTraceBlock(blockNumber uint64) ([]*raw.CallTrace, error) {
	// genesis block has no traces
	if blockNumber == 0 {
		return nil, nil
	}

	hexBlockNumber := "0x" + fmt.Sprintf("%x", blockNumber)
	stringPayload := fmt.Sprintf("{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"debug_traceBlockByNumber\",\"params\":[\"%s\",{\"tracer\": \"callTracer\", \"timeout\":\"300s\"}]}", hexBlockNumber)
	reqPayload := strings.NewReader(stringPayload)
	req, err := http.NewRequest("POST", c.url, reqPayload)
	if err != nil {
		return nil, err
	}

	req.Header.Add("accept", "application/json")
	req.Header.Add("content-type", "application/json")

	ctx, cancel := context.WithTimeout(req.Context(), 300*time.Second)
	defer cancel()

	req = req.WithContext(ctx)
	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	jrpcResult := &jrpcTraceResult{}
	err = json.Unmarshal(body, jrpcResult)
	if err != nil {
		return nil, err
	}
	if jrpcResult.Error != nil {
		return nil, fmt.Errorf("%v", jrpcResult.Error)
	}

	var rawTraces []*raw.CallTrace
	for _, trace := range jrpcResult.Result {
		if trace.Error != nil {
			return nil, fmt.Errorf("%v", trace.Error)
		}
		rawTrace := &raw.CallTrace{}
		err = protojson.Unmarshal(trace.Result, rawTrace)
		if err != nil {
			return nil, err
		}
		rawTraces = append(rawTraces, rawTrace)
	}

	return rawTraces, nil
}

func (c *client) GetBlockReceipt(blockNumber uint64) ([]*raw.TransactionReceipt, error) {
	hexBlockNumber := "0x" + fmt.Sprintf("%x", blockNumber)
	stringPayload := fmt.Sprintf("{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockReceipts\",\"params\":[\"%s\"]}", hexBlockNumber)
	reqPayload := strings.NewReader(stringPayload)
	req, err := http.NewRequest("POST", c.url, reqPayload)
	if err != nil {
		return nil, err
	}

	req.Header.Add("accept", "application/json")
	req.Header.Add("content-type", "application/json")

	ctx, cancel := context.WithTimeout(req.Context(), 10*time.Second)
	defer cancel()

	req = req.WithContext(ctx)
	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	jrpcResult := &jrpcBlockReceiptsResult{}
	err = json.Unmarshal(body, jrpcResult)
	if err != nil {
		return nil, err
	}
	if jrpcResult.Error != nil {
		return nil, fmt.Errorf("%v", jrpcResult.Error)
	}

	var rawReceipts []*raw.TransactionReceipt
	for _, receipt := range jrpcResult.Result {
		rawReceipt := &raw.TransactionReceipt{}
		err = protojson.Unmarshal(receipt, rawReceipt)
		if err != nil {
			return nil, err
		}
		rawReceipts = append(rawReceipts, rawReceipt)
	}

	return rawReceipts, nil

}

func (c *client) GetTransactionReceipt(txHash string) (*raw.TransactionReceipt, error) {
	stringPayload := fmt.Sprintf("{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"eth_getTransactionReceipt\",\"params\":[\"%s\"]}", txHash)
	reqPayload := strings.NewReader(stringPayload)
	req, err := http.NewRequest("POST", c.url, reqPayload)
	if err != nil {
		return nil, err
	}

	req.Header.Add("accept", "application/json")
	req.Header.Add("content-type", "application/json")

	ctx, cancel := context.WithTimeout(req.Context(), 10*time.Second)
	defer cancel()

	req = req.WithContext(ctx)
	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	jrpcResult := &jrpcTxReceiptResult{}
	err = json.Unmarshal(body, jrpcResult)
	if err != nil {
		return nil, err
	}
	if jrpcResult.Error != nil {
		return nil, fmt.Errorf("%v", jrpcResult.Error)
	}

	rawReceipt := &raw.TransactionReceipt{}
	err = protojson.Unmarshal(jrpcResult.Result, rawReceipt)
	if err != nil {
		return nil, err
	}

	return rawReceipt, nil
}
