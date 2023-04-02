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
	"github.com/xitongsys/parquet-go/writer"
)

type GCSConnector struct {
	bucketName string
	projectID  string
	rangeSize  int
	bucket     *storage.BucketHandle
}

func NewGCSConnector(ctx context.Context, bucketName string, projectID string) (*GCSConnector, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &GCSConnector{
		bucketName: bucketName,
		projectID:  projectID,
		bucket:     client.Bucket(bucketName),
	}, nil
}

func MustNewGCSConnector(ctx context.Context, bucketName string, projectID string, logger framework.Logger) *GCSConnector {
	client, err := storage.NewClient(ctx)
	if err != nil {
		logger.Fatalf("Could not instantiate GCS client: %v", err)
	}

	return &GCSConnector{
		bucketName: bucketName,
		projectID:  projectID,
		bucket:     client.Bucket(bucketName),
	}
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
	obj := g.bucket.Object(filename)

	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	// @TODO - need to read parquet file and output
	return &model.ParquetBlock{}, nil
}
