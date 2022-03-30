package s3spanstore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
)

var testDedupeRewriteBufferDuration = 50 * time.Millisecond

func NewTestDedupeParquetWriter(assert *assert.Assertions, parquetWriter IParquetWriter) *DedupeParquetWriter {
	loggerName := "jaeger-s3"

	logLevel := os.Getenv("GRPC_STORAGE_PLUGIN_LOG_LEVEL")
	if logLevel == "" {
		logLevel = hclog.Debug.String()
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.LevelFromString(logLevel),
		Name:       loggerName,
		JSONFormat: true,
	})

	writer, err := NewDedupeParquetWriter(logger, 100*time.Millisecond, testDedupeRewriteBufferDuration, 100, parquetWriter)
	assert.NoError(err)

	return writer
}

type testWriter struct {
	writes []interface{}
}

type writeItem struct {
	row            interface{}
	maxBufferUntil time.Time
}

func (w *testWriter) Write(ctx context.Context, time time.Time, maxBufferUntil time.Time, row interface{}) error {
	w.writes = append(w.writes, writeItem{row: row, maxBufferUntil: maxBufferUntil})
	return nil
}

func (w *testWriter) Close() error {
	return nil
}

type operation struct {
	name string
}

func (o operation) DedupeKey() string {
	return o.name
}

func TestDedupeParquetWriter(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	testWriter := &testWriter{writes: []interface{}{}}
	writer := NewTestDedupeParquetWriter(assert, testWriter)

	timeNow := time.Now()

	assert.NoError(writer.Write(ctx, timeNow, timeNow, operation{name: "a"}))
	assert.NoError(writer.Write(ctx, timeNow, timeNow, operation{name: "a"}))
	assert.NoError(writer.Write(ctx, timeNow, timeNow, operation{name: "b"}))

	assert.Equal([]interface{}{
		writeItem{row: operation{name: "a"}, maxBufferUntil: timeNow},
		writeItem{row: operation{name: "b"}, maxBufferUntil: timeNow},
	}, testWriter.writes)
}

func TestDedupeParquetWriterWritesAgain(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	testWriter := &testWriter{writes: []interface{}{}}
	writer := NewTestDedupeParquetWriter(assert, testWriter)

	timeNow := time.Now()
	futureTimeNow := timeNow.Add(200 * time.Millisecond)

	assert.NoError(writer.Write(ctx, timeNow, timeNow, operation{name: "a"}))
	assert.NoError(writer.Write(ctx, futureTimeNow, futureTimeNow, operation{name: "a"}))

	assert.Equal([]interface{}{
		writeItem{row: operation{name: "a"}, maxBufferUntil: timeNow},
		writeItem{row: operation{name: "a"}, maxBufferUntil: futureTimeNow.Add(testDedupeRewriteBufferDuration)},
	}, testWriter.writes)
}
