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

	spanParquetWriter *ParquetWriter
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

func NewWriter(logger hclog.Logger, svc S3API, s3Config config.S3) (*Writer, error) {
	rand.Seed(time.Now().UnixNano())

	bufferDuration := time.Second * 60
	if s3Config.BufferDuration != "" {
		duration, err := time.ParseDuration(s3Config.BufferDuration)
		if err != nil {
			return nil, fmt.Errorf("failed to parse buffer duration: %w", err)
		}
		bufferDuration = duration
	}

	ctx := context.Background()

	if s3Config.EmptyBucket {
		if err := EmptyBucket(ctx, svc, s3Config.BucketName); err != nil {
			return nil, fmt.Errorf("failed to empty s3 bucket: %w", err)
		}
	}

	spanParquetWriter, err := NewParquetWriter(ctx, logger, svc, bufferDuration, s3Config.BucketName, s3Config.Prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	w := &Writer{
		logger:            logger,
		spanParquetWriter: spanParquetWriter,
	}

	return w, nil
}

func (w *Writer) WriteSpan(ctx context.Context, span *model.Span) error {
	// s.logger.Debug("WriteSpan", span)

	spanRecord, err := NewSpanRecordFromSpan(span)
	if err != nil {
		return fmt.Errorf("failed to create span record: %w", err)
	}

	if err := w.spanParquetWriter.Write(ctx, span.StartTime, spanRecord); err != nil {
		return fmt.Errorf("failed to write span item: %w", err)
	}
	return nil
}

func (w *Writer) Close() error {
	return w.spanParquetWriter.Close()
}
