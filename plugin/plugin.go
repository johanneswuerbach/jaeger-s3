package plugin

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/johanneswuerbach/jaeger-s3/plugin/config"
	"github.com/johanneswuerbach/jaeger-s3/plugin/s3spanstore"
	"golang.org/x/sync/errgroup"
)

var (
	_ shared.StoragePlugin             = (*S3Plugin)(nil)
	_ shared.StreamingSpanWriterPlugin = (*S3Plugin)(nil)
	_ io.Closer                        = (*S3Plugin)(nil)
)

func NewS3Plugin(ctx context.Context, logger hclog.Logger, s3Svc *s3.Client, s3Config config.S3, athenaSvc *athena.Client, athenaConfig config.Athena) (*S3Plugin, error) {
	spanWriter, err := s3spanstore.NewWriter(ctx, logger, s3Svc, s3Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create span writer, %v", err)
	}

	spanReader, err := s3spanstore.NewReader(ctx, logger, athenaSvc, athenaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create span reader, %v", err)
	}

	return &S3Plugin{
		spanWriter: spanWriter,
		spanReader: spanReader,
		logger:     logger,
	}, nil
}

type S3Plugin struct {
	spanWriter *s3spanstore.Writer
	spanReader *s3spanstore.Reader

	logger hclog.Logger
}

func (h *S3Plugin) SpanWriter() spanstore.Writer {
	return h.spanWriter
}

func (h *S3Plugin) SpanReader() spanstore.Reader {
	return h.spanReader
}

func (h *S3Plugin) DependencyReader() dependencystore.Reader {
	return h.spanReader
}

func (h *S3Plugin) StreamingSpanWriter() spanstore.Writer {
	return h.spanWriter
}

func (h *S3Plugin) Close() error {
	g := errgroup.Group{}

	g.Go(h.spanWriter.Close)
	g.Go(h.spanReader.Close)

	return g.Wait()
}
