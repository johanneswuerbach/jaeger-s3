package s3spanstore

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/johanneswuerbach/jaeger-s3/plugin/config"
	"golang.org/x/sync/errgroup"
)

// mockgen -destination=./plugin/s3spanstore/mocks/mock_s3.go -package=mocks github.com/johanneswuerbach/jaeger-s3/plugin/s3spanstore S3API

type S3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type Writer struct {
	logger hclog.Logger

	spanParquetWriter       IParquetWriter
	operationsParquetWriter *DedupeParquetWriter
}

func EmptyBucket(ctx context.Context, svc S3API, bucketName string) error {
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	paginator := s3.NewListObjectsV2Paginator(svc, params)

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed fetch page: %w", err)
		}
		for _, value := range output.Contents {
			if _, err := svc.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    value.Key,
			}); err != nil {
				return fmt.Errorf("failed to delete object: %w", err)
			}
		}
	}

	return nil
}

func NewWriter(ctx context.Context, logger hclog.Logger, svc S3API, s3Config config.S3) (*Writer, error) {
	rand.Seed(time.Now().UnixNano())

	bufferDuration := time.Second * 60
	if s3Config.BufferDuration != "" {
		duration, err := time.ParseDuration(s3Config.BufferDuration)
		if err != nil {
			return nil, fmt.Errorf("failed to parse buffer duration: %w", err)
		}
		bufferDuration = duration
	}

	operationsDedupeDuration := time.Hour * 1
	if s3Config.OperationsDedupeDuration != "" {
		duration, err := time.ParseDuration(s3Config.OperationsDedupeDuration)
		if err != nil {
			return nil, fmt.Errorf("failed to parse buffer duration: %w", err)
		}
		operationsDedupeDuration = duration
	}

	operationsDedupeCacheSize := 10000
	if s3Config.OperationsDedupeCacheSize > 0 {
		operationsDedupeCacheSize = s3Config.OperationsDedupeCacheSize
	}

	if s3Config.EmptyBucket {
		if err := EmptyBucket(ctx, svc, s3Config.BucketName); err != nil {
			return nil, fmt.Errorf("failed to empty s3 bucket: %w", err)
		}
	}

	spanParquetWriter, err := NewParquetWriter(ctx, logger, svc, bufferDuration, s3Config.BucketName, s3Config.SpansPrefix, new(SpanRecord))
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	operationsParquetWriter, err := NewParquetWriter(ctx, logger, svc, bufferDuration, s3Config.BucketName, s3Config.OperationsPrefix, new(OperationRecord))
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	operationsDedupeParquetWriter, err := NewDedupeParquetWriter(logger, operationsDedupeDuration, operationsDedupeCacheSize, operationsParquetWriter)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	w := &Writer{
		logger:                  logger,
		operationsParquetWriter: operationsDedupeParquetWriter,
		spanParquetWriter:       spanParquetWriter,
	}

	return w, nil
}

func (w *Writer) WriteSpan(ctx context.Context, span *model.Span) error {
	// s.logger.Debug("WriteSpan", span)

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		operationRecord, err := NewOperationRecordFromSpan(span)
		if err != nil {
			return fmt.Errorf("failed to create operation record: %w", err)
		}

		if err := w.operationsParquetWriter.Write(gCtx, span.StartTime, operationRecord); err != nil {
			return fmt.Errorf("failed to write operation item: %w", err)
		}

		return nil
	})

	g.Go(func() error {
		spanRecord, err := NewSpanRecordFromSpan(span)
		if err != nil {
			return fmt.Errorf("failed to create span record: %w", err)
		}

		if err := w.spanParquetWriter.Write(gCtx, span.StartTime, spanRecord); err != nil {
			return fmt.Errorf("failed to write span item: %w", err)
		}

		return nil
	})

	return g.Wait()
}

func (w *Writer) Close() error {
	g := errgroup.Group{}

	g.Go(func() error {
		if err := w.spanParquetWriter.Close(); err != nil {
			return fmt.Errorf("failed to close parquet writer: %w", err)
		}

		return nil
	})

	g.Go(func() error {
		if err := w.operationsParquetWriter.Close(); err != nil {
			return fmt.Errorf("failed to close parquet writer: %w", err)
		}

		return nil
	})

	return g.Wait()
}
