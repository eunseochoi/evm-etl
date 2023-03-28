package ethereum

import (
	"context"
	"fmt"
	"github.com/datadaodevs/evm-etl/protos/go/protos/evm/raw"
	"github.com/datadaodevs/go-service-framework/pool"
	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go-source/gcs"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"strconv"
	"strings"
	"sync"
)

const (
	rangePrefix = "blocks_"
)

type callTraceNode struct {
	CallTrace  *raw.CallTrace
	Index      int64
	ParentHash string
}

func (e *EthereumDriver) Writers() []pool.FeedTransformer {
	return []pool.FeedTransformer{
		e.parquetAndUploadBlock,
		e.parquetAndUploadTransactions,
		e.parquetAndUploadTraces,
		e.parquetAndUploadLogs,
	}
}

func (e *EthereumDriver) parquetAndUploadBlock(res interface{}) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		block, blockNumber, err := unpackBlock(res)
		if err != nil {
			return nil, err
		}

		filename := fmt.Sprintf("blocks/%s/%d.parquet", rangeName(blockNumber, e.config.DirectoryRange), blockNumber)
		if err := e.write(ctx, ProtoBlockToParquet(block.Block), &ParquetBlock{}, filename); err != nil {
			return nil, err
		}

		e.logger.Infof("successfully parqueted block for %d", blockNumber)
		return nil, nil
	}
}

func (e *EthereumDriver) parquetAndUploadTransactions(res interface{}) pool.Runner {
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

		filename := fmt.Sprintf("transactions/%s/%d.parquet", rangeName(blockNumber, e.config.DirectoryRange), blockNumber)

		if err := e.writeMany(ctx, outputs, &ParquetTransaction{}, filename); err != nil {
			return nil, err
		}
		e.logger.Infof("successfully parqueted transactions for %d", blockNumber)

		return nil, nil
	}
}

func (e *EthereumDriver) parquetAndUploadTraces(res interface{}) pool.Runner {
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

		filename := fmt.Sprintf("logs/%s/%d.parquet", rangeName(blockNumber, e.config.DirectoryRange), blockNumber)

		if err := e.writeMany(ctx, outputs, &ParquetLog{}, filename); err != nil {
			return nil, err
		}
		e.logger.Infof("successfully parqueted traces for %d", blockNumber)

		return nil, nil
	}
}

func (e *EthereumDriver) parquetAndUploadLogs(res interface{}) pool.Runner {
	return func(ctx context.Context) (interface{}, error) {
		block, blockNumber, err := unpackBlock(res)
		if err != nil {
			return nil, err
		}

		if len(block.Block.Transactions) == 0 || len(block.CallTraces) == 0 {
			return nil, nil
		}

		var bfsWG sync.WaitGroup
		var outputs []interface{}
		mutex := sync.Mutex{}
		for i, callTrace := range block.CallTraces {
			bfsWG.Add(1)
			go func(index int, callTrace *raw.CallTrace) {
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

		filename := fmt.Sprintf("traces/%s/%d.parquet", rangeName(blockNumber, e.config.DirectoryRange), blockNumber)
		if err := e.writeMany(ctx, outputs, &ParquetTrace{}, filename); err != nil {
			return nil, err
		}

		e.logger.Infof("successfully parqueted logs for %d", blockNumber)

		return nil, nil
	}
}

func unpackBlock(res interface{}) (*raw.Data, int64, error) {
	obj, ok := res.(*raw.Data)
	if !ok {
		return nil, 0, errors.New("Result is not correct type")
	}

	hexBlockNumber := strings.Replace(obj.Block.Number, "0x", "", -1)
	blockNumber, err := strconv.ParseInt(hexBlockNumber, 16, 64)
	if err != nil {
		return nil, 0, err
	}

	return obj, blockNumber, nil
}

func (e *EthereumDriver) write(ctx context.Context, input interface{}, mapToStruct interface{}, filename string) error {
	gw, err := gcs.NewGcsFileWriter(
		ctx,
		e.config.GCPProjectID,
		e.config.BucketName,
		filename,
	)
	if err != nil {
		return errors.Errorf("cannot open file: %v", err)
	}
	defer gw.Close()

	pw, err := writer.NewParquetWriter(gw, mapToStruct, 4)
	if err != nil {
		return errors.Errorf("cannot create parquet writer: %v", err)
	}

	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	if err = pw.Write(input); err != nil {
		return errors.Errorf("write error: %v", err)
	}

	if err = pw.WriteStop(); err != nil {
		return errors.Errorf("WriteStop error: %v", err)
	}
	return nil
}

func (e *EthereumDriver) writeMany(ctx context.Context, input []interface{}, mapToStruct interface{}, filename string) error {
	gw, err := gcs.NewGcsFileWriter(
		ctx,
		e.config.GCPProjectID,
		e.config.BucketName,
		filename,
	)
	if err != nil {
		return errors.Errorf("cannot open file: %v", err)
	}
	defer gw.Close()

	pw, err := writer.NewParquetWriter(gw, mapToStruct, 4)
	if err != nil {
		return errors.Errorf("cannot create parquet writer: %v", err)
	}

	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	for _, row := range input {
		if err = pw.Write(row); err != nil {
			return errors.Errorf("write error: %v", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		return errors.Errorf("WriteStop error: %v", err)
	}
	return nil
}

func rangeName(height int64, directoryRange int) string {
	rangeSize := int64(directoryRange)
	bottom := (height / rangeSize) * rangeSize
	top := bottom + rangeSize - 1
	return fmt.Sprintf("%s%d-%d", rangePrefix, bottom, top)
}
