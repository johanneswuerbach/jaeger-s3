package s3spanstore

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/johanneswuerbach/jaeger-s3/plugin/config"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
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
}

const (
	PARQUET_CONCURRENCY = 1
)

var (
	PARQUET_ROTATION_INTERVAL = 60 * time.Second
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

type Writer struct {
	logger     hclog.Logger
	svc        S3API
	bucketName string
	prefix     string
	ticker     *time.Ticker
	done       chan bool

	parquetWriteFile source.ParquetFile
	parquetWriter    *writer.ParquetWriter
	bufferMutex      sync.Mutex
}

func NewWriter(logger hclog.Logger, svc S3API, s3Config config.S3) (*Writer, error) {
	rand.Seed(time.Now().UnixNano())

	w := &Writer{
		svc:        svc,
		bucketName: s3Config.BucketName,
		prefix:     s3Config.Prefix,
		logger:     logger,
		ticker:     time.NewTicker(PARQUET_ROTATION_INTERVAL),
		done:       make(chan bool),
	}

	ctx := context.Background()

	go func() {
		for {
			select {
			case <-w.done:
				return
			case <-w.ticker.C:
				if err := w.rotateParquetWriter(ctx); err != nil {
					w.logger.Error("failed to rotate parquet writer", err)
				}
			}
		}
	}()

	return w, nil
}

func (w *Writer) ensureParquetWriter(ctx context.Context) error {
	if w.parquetWriter != nil {
		return nil
	}

	writeFile, err := s3v2.NewS3FileWriterWithClient(ctx, w.svc, w.bucketName, w.parquetKey(), nil)
	if err != nil {
		return fmt.Errorf("failed to create parquet s3 client: %w", err)
	}
	w.parquetWriteFile = writeFile

	parquetWriter, err := writer.NewParquetWriter(writeFile, new(SpanRecord), PARQUET_CONCURRENCY)
	if err != nil {
		w.parquetWriteFile.Close()
		w.parquetWriteFile = nil
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	w.parquetWriter = parquetWriter

	return nil
}

func (w *Writer) parquetKey() string {
	t := time.Now()

	return w.prefix + t.Format("2006/01/02/03") + "/" + RandStringBytes(32) + ".parquet"
}

func (w *Writer) closeParquetWriter(parquetWriter *writer.ParquetWriter, parquetWriteFile source.ParquetFile) error {
	if parquetWriter != nil {
		if err := parquetWriter.WriteStop(); err != nil {
			return fmt.Errorf("parquet write stop error: %w", err)
		}
	}

	if parquetWriteFile != nil {
		if err := parquetWriteFile.Close(); err != nil {
			return fmt.Errorf("parquet file write close error: %w", err)
		}
	}

	return nil
}

func (w *Writer) rotateParquetWriter(ctx context.Context) error {
	w.bufferMutex.Lock()

	parquetWriteFile := w.parquetWriteFile
	parquetWriter := w.parquetWriter
	w.parquetWriteFile = nil
	w.parquetWriter = nil

	w.bufferMutex.Unlock()

	if err := w.closeParquetWriter(parquetWriter, parquetWriteFile); err != nil {
		return fmt.Errorf("failed to close previous parquet writer: %w", err)
	}

	return nil
}

func (w *Writer) WriteSpan(ctx context.Context, span *model.Span) error {
	// s.logger.Debug("WriteSpan", span)

	spanRecord, err := NewSpanRecordFromSpan(span)
	if err != nil {
		return fmt.Errorf("failed to create span record: %w", err)
	}

	w.bufferMutex.Lock()
	defer w.bufferMutex.Unlock()

	if err := w.ensureParquetWriter(ctx); err != nil {
		return fmt.Errorf("failed to ensure parquet writer: %w", err)
	}

	if err := w.parquetWriter.Write(spanRecord); err != nil {
		return fmt.Errorf("failed to write span item: %w", err)
	}
	return nil
}

func (w *Writer) Close() error {
	w.ticker.Stop()
	w.done <- true

	w.bufferMutex.Lock()
	defer w.bufferMutex.Unlock()

	return w.closeParquetWriter(w.parquetWriter, w.parquetWriteFile)
}
