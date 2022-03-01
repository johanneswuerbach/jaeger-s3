package s3spanstore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
)

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

	writer, err := NewDedupeParquetWriter(logger, 100*time.Millisecond, 100, parquetWriter)
	assert.NoError(err)

	return writer
}

type testWriter struct {
	writes []interface{}
}

func (w *testWriter) Write(ctx context.Context, time time.Time, row interface{}) error {
	fmt.Println("Write", row)
	w.writes = append(w.writes, row)
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

	time := time.Now()

	assert.NoError(writer.Write(ctx, time, operation{name: "a"}))
	assert.NoError(writer.Write(ctx, time, operation{name: "a"}))
	assert.NoError(writer.Write(ctx, time, operation{name: "b"}))

	assert.Equal([]interface{}{
		operation{name: "a"},
		operation{name: "b"},
	}, testWriter.writes)
}

func TestDedupeParquetWriterWritesAgain(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	testWriter := &testWriter{writes: []interface{}{}}
	writer := NewTestDedupeParquetWriter(assert, testWriter)

	timeNow := time.Now()

	assert.NoError(writer.Write(ctx, timeNow, operation{name: "a"}))
	assert.NoError(writer.Write(ctx, timeNow.Add(200*time.Millisecond), operation{name: "a"}))

	assert.Equal([]interface{}{
		operation{name: "a"},
		operation{name: "a"},
	}, testWriter.writes)
}
