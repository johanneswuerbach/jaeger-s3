package s3spanstore

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

const (
	PARQUET_CONCURRENCY = 1
	PARTION_FORMAT      = "2006/01/02/15"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func S3ParquetKey(prefix, suffix string, datehour string) string {
	return prefix + datehour + "/" + suffix + ".parquet"
}

func S3PartitionKey(t time.Time) string {
	return t.Format(PARTION_FORMAT)
}

type ParquetRef struct {
	parquetWriteFile source.ParquetFile
	parquetWriter    *writer.ParquetWriter
}

type ParquetWriter struct {
	logger     hclog.Logger
	svc        S3API
	bucketName string
	prefix     string
	ticker     *time.Ticker
	done       chan bool

	parquetWriterRefs map[string]*ParquetRef
	bufferMutex       sync.Mutex
	ctx               context.Context
}

func NewParquetWriter(ctx context.Context, logger hclog.Logger, svc S3API, bufferDuration time.Duration, bucketName string, prefix string) (*ParquetWriter, error) {
	w := &ParquetWriter{
		svc:               svc,
		bucketName:        bucketName,
		prefix:            prefix,
		logger:            logger,
		ticker:            time.NewTicker(bufferDuration),
		done:              make(chan bool),
		parquetWriterRefs: map[string]*ParquetRef{},
		ctx:               ctx,
	}

	go func() {
		for {
			select {
			case <-w.done:
				return
			case <-w.ticker.C:
				if err := w.rotateParquetWriters(); err != nil {
					w.logger.Error("failed to rotate parquet writer", err)
				}
			}
		}
	}()

	return w, nil
}

func (w *ParquetWriter) getParquetWriter(datehour string) (*writer.ParquetWriter, error) {
	if w.parquetWriterRefs[datehour] != nil {
		return w.parquetWriterRefs[datehour].parquetWriter, nil
	}

	writeFile, err := s3v2.NewS3FileWriterWithClient(w.ctx, w.svc, w.bucketName, w.parquetKey(datehour), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet s3 client: %w", err)
	}

	parquetWriter, err := writer.NewParquetWriter(writeFile, new(SpanRecord), PARQUET_CONCURRENCY)
	if err != nil {
		writeFile.Close()
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	w.parquetWriterRefs[datehour] = &ParquetRef{
		parquetWriteFile: writeFile,
		parquetWriter:    parquetWriter,
	}

	return parquetWriter, nil
}

func (w *ParquetWriter) parquetKey(datehour string) string {
	return S3ParquetKey(w.prefix, RandStringBytes(32), datehour)
}

func (w *ParquetWriter) closeParquetWriter(parquetRef *ParquetRef) error {
	if parquetRef.parquetWriter != nil {
		if err := parquetRef.parquetWriter.WriteStop(); err != nil {
			return fmt.Errorf("parquet write stop error: %w", err)
		}
	}

	if parquetRef.parquetWriteFile != nil {
		if err := parquetRef.parquetWriteFile.Close(); err != nil {
			return fmt.Errorf("parquet file write close error: %w", err)
		}
	}

	return nil
}

func (w *ParquetWriter) rotateParquetWriters() error {
	w.bufferMutex.Lock()

	writerRefs := w.parquetWriterRefs
	w.parquetWriterRefs = map[string]*ParquetRef{}

	w.bufferMutex.Unlock()

	return w.closeParquetWriters(writerRefs)
}

func (w *ParquetWriter) closeParquetWriters(parquetWriterRefs map[string]*ParquetRef) error {
	for _, writerRef := range parquetWriterRefs {
		if err := w.closeParquetWriter(writerRef); err != nil {
			return fmt.Errorf("failed to close previous parquet writer: %w", err)
		}
	}

	return nil
}

func (w *ParquetWriter) Write(ctx context.Context, time time.Time, row interface{}) error {
	w.bufferMutex.Lock()
	defer w.bufferMutex.Unlock()

	spanDatehour := S3PartitionKey(time)

	parquetWriter, err := w.getParquetWriter(spanDatehour)
	if err != nil {
		return fmt.Errorf("failed to get parquet writer: %w", err)
	}

	if err := parquetWriter.Write(row); err != nil {
		return fmt.Errorf("failed to write row: %w", err)
	}
	return nil
}

func (w *ParquetWriter) Close() error {
	w.ticker.Stop()
	w.done <- true

	w.bufferMutex.Lock()
	defer w.bufferMutex.Unlock()

	return w.closeParquetWriters(w.parquetWriterRefs)
}
