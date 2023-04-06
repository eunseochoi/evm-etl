package optimism

import (
	"context"
	"fmt"
	model "github.com/datadaodevs/evm-etl/model/optimism"
	protos "github.com/datadaodevs/evm-etl/protos/go/protos/chains/optimism"
	"github.com/datadaodevs/evm-etl/shared/util"
	"github.com/datadaodevs/go-service-framework/pool"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"sync"
)

const (
	rangePrefix = "blocks_"
	nullAddress = "0x0000000000000000000000000000000000000000"
)

// callTraceNode is a local utility struct for performing BFS-style traversal of trace tree
type callTraceNode struct {
	CallTrace  *protos.CallTrace
	Index      int64
	ParentHash string
}

// Writers defines a set of parallelizable write steps for processing a block and its children
func (d *OptimismDriver) Writers() []pool.FeedTransformer {
	return []pool.FeedTransformer{
		d.parquetAndUploadBlock,
		d.parquetAndUploadTransactions,
		d.parquetAndUploadTraces,
		d.parquetAndUploadLogs,
	}
}

// parquetAndUploadBlock writes parquet to storage for a block
func (d *OptimismDriver) parquetAndUploadBlock(res interface{}) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		block, blockNumber, err := unpackBlock(res)
		if err != nil {
			return nil, err
		}

		filename := fmt.Sprintf("blocks/%s/%d.parquet", util.RangeName(blockNumber, d.config.DirectoryRange), blockNumber)
		if err := d.store.innerStore.WriteOne(ctx, ProtoBlockToParquet(block.Block), &model.ParquetBlock{}, filename); err != nil {
			return nil, err
		}

		d.logger.Infof("successfully parqueted block for %d", blockNumber)
		return nil, nil
	}
}

// parquetAndUploadTransactions writes parquet to storage for transactions
func (d *OptimismDriver) parquetAndUploadTransactions(res interface{}) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		block, blockNumber, err := unpackBlock(res)
		if err != nil {
			return nil, err
		}

		if len(block.Block.Transactions) == 0 {
			return nil, nil
		}

		var outputs []interface{}
		for i, tx := range block.Block.Transactions {
			parquetTransaction, err := ProtoTransactionToParquet(tx, block.TransactionReceipts[i])
			if err != nil {
				return nil, err
			}
			outputs = append(outputs, parquetTransaction)
		}

		filename := fmt.Sprintf("transactions/%s/%d.parquet", util.RangeName(blockNumber, d.config.DirectoryRange), blockNumber)

		if err := d.store.innerStore.WriteMany(ctx, outputs, &model.ParquetTransaction{}, filename); err != nil {
			return nil, err
		}
		d.logger.Infof("successfully parqueted transactions for %d", blockNumber)

		return nil, nil
	}
}

// parquetAndUploadLogs writes parquet to storage for logs
func (d *OptimismDriver) parquetAndUploadLogs(res interface{}) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		block, blockNumber, err := unpackBlock(res)
		if err != nil {
			return nil, err
		}

		if len(block.Block.Transactions) == 0 {
			return nil, nil
		}

		var outputs []interface{}
		for _, receipt := range block.TransactionReceipts {
			for _, log := range receipt.Logs {
				outputs = append(outputs, ProtoLogToParquet(log))
			}
		}

		filename := fmt.Sprintf("logs/%s/%d.parquet", util.RangeName(blockNumber, d.config.DirectoryRange), blockNumber)

		if err := d.store.innerStore.WriteMany(ctx, outputs, &model.ParquetLog{}, filename); err != nil {
			return nil, err
		}
		d.logger.Infof("successfully parqueted traces for %d", blockNumber)

		return nil, nil
	}
}

// parquetAndUploadTraces writes parquet to storage for traces
func (d *OptimismDriver) parquetAndUploadTraces(res interface{}) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		block, blockNumber, err := unpackBlock(res)
		if err != nil {
			return nil, err
		}

		if len(block.Block.Transactions) == 0 || len(block.CallTraces) == 0 {
			return nil, nil
		}

		//	Filter null=>null transactions and ensure transaction and trace counts match
		filteredTx := filterNonTraceTransactions(block.Block.Transactions)
		if len(filteredTx) != len(block.CallTraces) {
			return nil, errors.Errorf("transactions and traces count don't match for block: %d %d != %d", blockNumber, len(filteredTx), len(block.CallTraces))
		}

		var bfsWG sync.WaitGroup
		var outputs []interface{}
		mutex := sync.Mutex{}
		for i, callTrace := range block.CallTraces {
			bfsWG.Add(1)
			go func(index int, callTrace *protos.CallTrace) {
				defer bfsWG.Done()

				queue := make([]*callTraceNode, 0)
				queue = append(
					queue,
					&callTraceNode{
						CallTrace:  callTrace,
						Index:      int64(index),
						ParentHash: "",
					},
				)
				for len(queue) > 0 {
					currentNode := queue[0]
					queue = queue[1:]
					currCallTrace := currentNode.CallTrace
					traceHash := hashCallTrace(currCallTrace)
					mutex.Lock()
					outputs = append(
						outputs,
						ProtoTraceToParquet(
							callTrace,
							block.Block.Transactions[index],
							traceHash,
							currentNode.ParentHash,
							currentNode.Index,
						),
					)
					mutex.Unlock()
					for callIndex, call := range currentNode.CallTrace.Calls {
						queue = append(
							queue,
							&callTraceNode{
								CallTrace:  call,
								Index:      int64(callIndex),
								ParentHash: traceHash,
							},
						)
					}

				}
			}(i, callTrace)
		}
		bfsWG.Wait()

		filename := fmt.Sprintf("traces/%s/%d.parquet", util.RangeName(blockNumber, d.config.DirectoryRange), blockNumber)
		if err := d.store.innerStore.WriteMany(ctx, outputs, &model.ParquetTrace{}, filename); err != nil {
			return nil, err
		}

		d.logger.Infof("successfully parqueted logs for %d", blockNumber)

		return nil, nil
	}
}

// unpackBlock pulls a block out of the generic response from the accumulator
func unpackBlock(res interface{}) (*protos.Data, uint64, error) {
	obj, ok := res.(*protos.Data)
	if !ok {
		return nil, 0, errors.New("Result is not correct type")
	}

	hexBlockNumber := strings.Replace(obj.Block.Number, "0x", "", -1)
	blockNumber, err := strconv.ParseInt(hexBlockNumber, 16, 64)
	if err != nil {
		return nil, 0, err
	}

	return obj, uint64(blockNumber), nil
}

func filterNonTraceTransactions(in []*protos.Transaction) []*protos.Transaction {
	var filteredTransactions []*protos.Transaction
	for _, tx := range in {
		if tx.From != nullAddress || tx.To != nullAddress {
			filteredTransactions = append(filteredTransactions, tx)
		}
	}
	return filteredTransactions
}
