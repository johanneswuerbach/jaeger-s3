package s3spanstore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/golang/mock/gomock"
	"github.com/hashicorp/go-hclog"
	"github.com/johanneswuerbach/jaeger-s3/plugin/config"
	"github.com/johanneswuerbach/jaeger-s3/plugin/s3spanstore/mocks"
	"github.com/stretchr/testify/assert"
)

func NewTestReader(ctx context.Context, assert *assert.Assertions, mockSvc *mocks.MockAthenaAPI) *Reader {
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

	reader, err := NewReader(logger, mockSvc, config.Athena{
		DatabaseName:         "default",
		TableName:            "jaeger",
		OutputLocation:       "s3://jaeger-s3-test-results/",
		WorkGroup:            "jaeger",
		MaxSpanAge:           "336h",
		DependenciesQueryTTL: "6h",
		ServicesQueryTTL:     "10s",
	})

	assert.NoError(err)

	return reader
}

func TestGetServices(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	queryID := "queryId"
	now := time.Now()
	serviceName := "test"

	mockSvc := mocks.NewMockAthenaAPI(ctrl)
	mockSvc.EXPECT().ListQueryExecutions(gomock.Any(), gomock.Any()).
		Return(&athena.ListQueryExecutionsOutput{}, nil)
	mockSvc.EXPECT().StartQueryExecution(gomock.Any(), gomock.Any()).
		Return(&athena.StartQueryExecutionOutput{
			QueryExecutionId: &queryID,
		}, nil)
	mockSvc.EXPECT().GetQueryExecution(gomock.Any(), gomock.Any()).
		Return(&athena.GetQueryExecutionOutput{
			QueryExecution: &types.QueryExecution{
				Status: &types.QueryExecutionStatus{
					CompletionDateTime: &now,
				},
			},
		}, nil)

	mockSvc.EXPECT().GetQueryResults(gomock.Any(), gomock.Any()).
		Return(&athena.GetQueryResultsOutput{
			ResultSet: &types.ResultSet{
				Rows: []types.Row{
					{},
					{Data: []types.Datum{
						{VarCharValue: &serviceName},
					}},
				},
			},
		}, nil)

	assert := assert.New(t)
	ctx := context.TODO()

	reader := NewTestReader(ctx, assert, mockSvc)

	services, err := reader.GetServices(ctx)

	assert.NoError(err)
	assert.Equal([]string{serviceName}, services)
}
