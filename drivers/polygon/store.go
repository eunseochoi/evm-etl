package polygon

import (
	"context"
	"errors"
	"fmt"
	model "github.com/coherentopensource/evm-etl/model/polygon"
	"github.com/coherentopensource/evm-etl/shared/storage"
	"github.com/coherentopensource/evm-etl/shared/util"
	"github.com/xitongsys/parquet-go-source/gcs"
	"github.com/xitongsys/parquet-go/reader"
)

type store struct {
	innerStore storage.Store
}

func (s *store) RetrieveBlock(ctx context.Context, blockHeight uint64) (*model.ParquetBlock, error) {
	filename := fmt.Sprintf("blocks/%s/%d.parquet", util.RangeName(blockHeight, s.innerStore.RangeSize()), blockHeight)
	fr, err := gcs.NewGcsFileReader(ctx, s.innerStore.ProjectID(), s.innerStore.Bucket(), filename)
	if err != nil {
		return nil, err
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(model.ParquetBlock), 4)
	if err != nil {
		return nil, err
	}
	defer pr.ReadStop()

	//	We expect 1 row - make sure there is at least 1 and take the first 1
	if pr.GetNumRows() == 0 {
		return nil, errors.New("no rows in block parquet")
	}

	block := make([]model.ParquetBlock, 1)
	if err = pr.Read(&block); err != nil {
		return nil, err
	}

	return &block[0], nil
}
