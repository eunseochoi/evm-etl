package storage

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	model "github.com/datadaodevs/evm-etl/model/ethereum"
	"github.com/datadaodevs/evm-etl/shared/util"
	framework "github.com/datadaodevs/go-service-framework/util"
	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go-source/gcs"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type GCSConnector struct {
	bucketName string
	projectID  string
	rangeSize  uint64
	bucket     *storage.BucketHandle
}

type GCSConfig struct {
	BucketName string `env:"gcs_bucket_name,required"`
	ProjectID  string `env:"gcp_project_id,required"`
	RangeSize  uint64 `env:"gcs_dir_range_size" envDefault:"10000"`
}

func NewGCSConnector(ctx context.Context, cfg *GCSConfig) (*GCSConnector, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &GCSConnector{
		bucketName: cfg.BucketName,
		projectID:  cfg.ProjectID,
		bucket:     client.Bucket(cfg.BucketName),
		rangeSize:  cfg.RangeSize,
	}, nil
}

func MustNewGCSConnector(ctx context.Context, cfg *GCSConfig, logger framework.Logger) *GCSConnector {
	client, err := NewGCSConnector(ctx, cfg)
	if err != nil {
		logger.Fatalf("Could not instantiate GCS client: %v", err)
	}

	return client
}

// Write writes a single parquet to GCS storage
func (g *GCSConnector) WriteOne(ctx context.Context, input interface{}, mapToStruct interface{}, filename string) error {
	gw, err := gcs.NewGcsFileWriter(
		ctx,
		g.projectID,
		g.bucketName,
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

// Write writes a mutliple parquets to GCS storage
func (g *GCSConnector) WriteMany(ctx context.Context, input []interface{}, mapToStruct interface{}, filename string) error {
	gw, err := gcs.NewGcsFileWriter(
		ctx,
		g.projectID,
		g.bucketName,
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

func (g *GCSConnector) RetrieveBlock(ctx context.Context, blockHeight uint64) (*model.ParquetBlock, error) {
	filename := fmt.Sprintf("blocks/%s/%d.parquet", util.RangeName(blockHeight, g.rangeSize), blockHeight)
	fr, err := gcs.NewGcsFileReader(ctx, g.projectID, g.bucketName, filename)
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
		return nil, errors.New("No rows in block parquet")
	}

	block := make([]model.ParquetBlock, 1)
	if err = pr.Read(&block); err != nil {
		return nil, err
	}

	return &block[0], nil
}
