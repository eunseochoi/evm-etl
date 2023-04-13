package storage

import (
	"cloud.google.com/go/storage"
	"context"
	framework "github.com/coherentopensource/go-service-framework/util"
	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go-source/gcs"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type GCSConnector struct {
	bucketName string
	projectID  string
	rangeSize  uint64
	bucket     *storage.BucketHandle
}

type GCSConfig struct {
	BucketName string `env:"GCS_BUCKET_NAME,required"`
	ProjectID  string `env:"GCP_PROJECT_ID,required"`
	RangeSize  uint64 `env:"GCS_DIR_RANGE_SIZE" envDefault:"10000"`
}

func NewGCSConnector(ctx context.Context, cfg *GCSConfig) (*GCSConnector, error) {
	return &GCSConnector{
		bucketName: cfg.BucketName,
		projectID:  cfg.ProjectID,
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
	var err error
	gw, err := gcs.NewGcsFileWriter(
		ctx,
		g.projectID,
		g.bucketName,
		filename,
	)
	if err != nil {
		return errors.Errorf("cannot open file: %v", err)
	}
	defer func() {
		closeErr := gw.Close()
		if closeErr != nil {
			if err != nil {
				err = errors.Wrapf(err, "GcsFileWriter Close error: %v", closeErr)
			} else {
				err = errors.Errorf("GcsFileWriter Close error: %v", closeErr)
			}
		}
	}()

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
	var err error
	gw, err := gcs.NewGcsFileWriter(
		ctx,
		g.projectID,
		g.bucketName,
		filename,
	)
	if err != nil {
		return errors.Errorf("cannot open file: %v", err)
	}
	defer func() {
		closeErr := gw.Close()
		if closeErr != nil {
			if err != nil {
				err = errors.Wrapf(err, "GcsFileWriter Close error: %v", closeErr)
			} else {
				err = errors.Errorf("GcsFileWriter Close error: %v", closeErr)
			}
		}
	}()

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

func (g *GCSConnector) ProjectID() string {
	return g.projectID
}

func (g *GCSConnector) Bucket() string {
	return g.bucketName
}

func (g *GCSConnector) RangeSize() uint64 {
	return g.rangeSize
}
