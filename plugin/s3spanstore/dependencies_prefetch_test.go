package s3spanstore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/assert"
)

func NewTestDependencyPrefetch(ctx context.Context, assert *assert.Assertions, reader ReaderWithDependencies, enabled bool) *DependenciesPrefetch {
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

	prefetch := NewDependenciesPrefetch(ctx, logger, reader, 100*time.Millisecond, enabled)
	prefetch.sleepDuration = time.Millisecond * 1

	return prefetch
}

type testReader struct {
	called int
}

func (r *testReader) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	r.called++
	return nil, nil
}

func TestDependenciesPrefetchEnabled(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	testReader := &testReader{called: 0}
	prefetch := NewTestDependencyPrefetch(ctx, assert, testReader, true)
	prefetch.Start()
	time.Sleep(10 * time.Millisecond)

	assert.Equal(1, testReader.called)

	time.Sleep(150 * time.Millisecond)

	assert.Equal(2, testReader.called)

	prefetch.Stop()
}

func TestDependenciesPrefetchDisabled(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	testReader := &testReader{called: 0}
	prefetch := NewTestDependencyPrefetch(ctx, assert, testReader, false)
	prefetch.Start()

	time.Sleep(150 * time.Millisecond)

	assert.Equal(0, testReader.called)

	prefetch.Stop()
}
