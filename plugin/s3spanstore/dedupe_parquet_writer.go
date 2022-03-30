package s3spanstore

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/go-hclog"
	lru "github.com/hashicorp/golang-lru"
)

type DedupeParquetWriter struct {
	logger                      hclog.Logger
	dedupeCache                 *lru.Cache
	dedupeDuration              time.Duration
	dedupeRewriteBufferDuration time.Duration
	parquetWriter               IParquetWriter
}

type DeduplicatableRow interface {
	DedupeKey() string
}

func NewDedupeParquetWriter(logger hclog.Logger, dedupeDuration time.Duration, dedupeRewriteBufferDuration time.Duration, dedupeCacheSize int, parquetWriter IParquetWriter) (*DedupeParquetWriter, error) {
	dedupeCache, err := lru.New(dedupeCacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create service cache, %v", err)
	}

	w := &DedupeParquetWriter{
		logger:                      logger,
		dedupeCache:                 dedupeCache,
		dedupeDuration:              dedupeDuration,
		dedupeRewriteBufferDuration: dedupeRewriteBufferDuration,
		parquetWriter:               parquetWriter,
	}

	return w, nil
}

func (w *DedupeParquetWriter) Write(ctx context.Context, rowTime time.Time, maxBufferUntil time.Time, row DeduplicatableRow) error {
	nextWriteTime, ok := w.dedupeCache.Get(row.DedupeKey())
	if ok && rowTime.Before(nextWriteTime.(time.Time)) {
		return nil
	}

	var maxBufferUntilCached time.Time
	if !ok {
		maxBufferUntilCached = maxBufferUntil
	} else {
		maxBufferUntilCached = maxBufferUntil.Add(w.dedupeRewriteBufferDuration)
	}

	if err := w.parquetWriter.Write(ctx, rowTime, maxBufferUntilCached, row); err != nil {
		return fmt.Errorf("failed to write row: %w", err)
	}
	w.dedupeCache.Add(row.DedupeKey(), rowTime.Add(w.dedupeDuration))

	return nil
}

func (w *DedupeParquetWriter) Close() error {
	return w.parquetWriter.Close()
}
