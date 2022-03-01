package s3spanstore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/golang/mock/gomock"
	"github.com/hashicorp/go-hclog"
	"github.com/johanneswuerbach/jaeger-s3/plugin/s3spanstore/mocks"
	"github.com/stretchr/testify/assert"
)

func NewTestParquetWriter(ctx context.Context, assert *assert.Assertions, mockSvc *mocks.MockS3API) *ParquetWriter {
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

	writer, err := NewParquetWriter(ctx, logger, mockSvc, time.Millisecond*200, "jaeger-spans", "/spans/")

	assert.NoError(err)

	return writer
}

func TestWriteSpanAndRotate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSvc := mocks.NewMockS3API(ctrl)
	mockSvc.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&s3.PutObjectOutput{}, nil).Times(2)

	assert := assert.New(t)
	ctx := context.TODO()

	writer := NewTestParquetWriter(ctx, assert, mockSvc)

	span := NewTestSpan(assert)

	spanRecord, err := NewSpanRecordFromSpan(span)
	assert.NoError(err)

	assert.NoError(writer.Write(ctx, span.StartTime, spanRecord))

	time.Sleep(time.Millisecond * 500)

	assert.NoError(writer.Write(ctx, span.StartTime, spanRecord))

	assert.NoError(writer.Close())
}
