package ethereum

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/datadaodevs/evm-etl/protos/go/protos/evm/raw"
	"github.com/pkg/errors"
)

func ProtoBlockToParquet(in *raw.Block) *ParquetBlock {
	out := ParquetBlock{
		Number:           in.Number,
		Hash:             in.Hash,
		ParentHash:       in.ParentHash,
		Nonce:            in.Nonce,
		SHA3Uncles:       in.Sha3Uncles,
		LogsBloom:        in.LogsBloom,
		TransactionsRoot: in.TransactionsRoot,
		StateRoot:        in.StateRoot,
		ReceiptsRoot:     in.ReceiptsRoot,
		Miner:            in.Miner,
		Difficulty:       in.Difficulty,
		TotalDifficulty:  in.TotalDifficulty,
		ExtraData:        in.ExtraData,
		Size:             in.Size,
		GasLimit:         in.GasLimit,
		GasUsed:          in.GasUsed,
		Timestamp:        in.Timestamp,
		BaseFeePerGas:    in.BaseFeePerGas,
		MixHash:          in.MixHash,
	}

	for _, uncle := range in.Uncles {
		out.Uncles = append(out.Uncles, uncle)
	}

	return &out
}

func ProtoTransactionToParquet(inTx *raw.Transaction, inReceipt *raw.TransactionReceipt) (*ParquetTransaction, error) {
	out := ParquetTransaction{
		BlockNumber:          inTx.BlockNumber,
		BlockHash:            inTx.BlockHash,
		Hash:                 inTx.Hash,
		From:                 inTx.From,
		To:                   inTx.To,
		Value:                inTx.Value,
		Gas:                  inTx.Gas,
		GasPrice:             inTx.GasPrice,
		Input:                inTx.Input,
		Type:                 inTx.Type,
		Nonce:                inTx.Nonce,
		TransactionIndex:     inTx.TransactionIndex,
		V:                    inTx.V,
		R:                    inTx.R,
		S:                    inTx.S,
		MaxFeePerGas:         inTx.MaxPriorityFeePerGas,
		MaxPriorityFeePerGas: inTx.MaxPriorityFeePerGas,
		CumulativeGasUsed:    inReceipt.CumulativeGasUsed,
		EffectiveGasPrice:    inReceipt.EffectiveGasPrice,
		GasUsed:              inReceipt.GasUsed,
		LogsBloom:            inReceipt.LogsBloom,
		Status:               inReceipt.Status,
	}

	for _, access := range inTx.AccessList {
		accessJSON, err := json.Marshal(access)
		if err != nil {
			return nil, errors.Errorf("failed to convert struct to json: %v", err)
		}
		out.AccessList = append(out.AccessList, string(accessJSON))
	}

	return &out, nil
}

func ProtoLogToParquet(in *raw.Log) *ParquetLog {
	out := ParquetLog{
		BlockNumber:      in.BlockNumber,
		BlockHash:        in.BlockHash,
		TransactionHash:  in.TransactionHash,
		TransactionIndex: in.TransactionIndex,
		LogIndex:         in.LogIndex,
		Address:          in.Address,
		Data:             in.Data,
		Removed:          in.Removed,
	}
	for _, topic := range in.Topics {
		out.Topics = append(out.Topics, topic)
	}

	return &out
}

func ProtoTraceToParquet(inTrace *raw.CallTrace, inTransaction *raw.Transaction, hash string, parentHash string, index int64) *ParquetTrace {
	return &ParquetTrace{
		BlockNumber:     inTransaction.BlockNumber,
		BlockHash:       inTransaction.BlockHash,
		TransactionHash: inTransaction.Hash,
		Hash:            hash,
		ParentHash:      parentHash,
		Index:           index,
		Type:            inTrace.Type,
		From:            inTrace.From,
		To:              inTrace.To,
		Value:           inTrace.Value,
		Gas:             inTrace.Gas,
		GasUsed:         inTrace.GasUsed,
		Input:           inTrace.Input,
		Output:          inTrace.Output,
		Error:           inTrace.Error,
		RevertReason:    inTrace.RevertReason,
	}
}

func hashCallTrace(callTrace *raw.CallTrace) string {
	hasher := sha256.New()
	hasher.Write([]byte(fmt.Sprintf("%v", callTrace)))

	return hex.EncodeToString(hasher.Sum(nil))
}
