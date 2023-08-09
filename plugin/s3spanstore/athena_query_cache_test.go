package s3spanstore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/golang/mock/gomock"
	"github.com/hashicorp/go-hclog"
	"github.com/johanneswuerbach/jaeger-s3/plugin/s3spanstore/mocks"
	"github.com/stretchr/testify/assert"
)

func NewTestAthenaQueryCache(mockSvc *mocks.MockAthenaAPI) *AthenaQueryCache {
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

	return NewAthenaQueryCache(logger, mockSvc, "jaeger")
}

func TestNoResults(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	assert := assert.New(t)
	ctx := context.TODO()

	mockSvc := mocks.NewMockAthenaAPI(ctrl)
	mockSvc.EXPECT().ListQueryExecutions(gomock.Any(), gomock.Any()).
		Return(&athena.ListQueryExecutionsOutput{}, nil)
	cache := NewTestAthenaQueryCache(mockSvc)

	cachedQuery, err := cache.Lookup(ctx, "test", time.Second*60)

	assert.NoError(err)
	assert.Nil(cachedQuery)
}

func TestOneFinishedResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	validQueryID := "get-services"
	invalidQueryID := "different"

	assert := assert.New(t)
	ctx := context.TODO()

	mockSvc := mocks.NewMockAthenaAPI(ctrl)
	mockSvc.EXPECT().ListQueryExecutions(gomock.Any(), gomock.Any()).
		Return(&athena.ListQueryExecutionsOutput{
			QueryExecutionIds: []string{invalidQueryID, validQueryID},
		}, nil)

	mockSvc.EXPECT().BatchGetQueryExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *athena.BatchGetQueryExecutionInput, _ ...func(*athena.Options)) (*athena.BatchGetQueryExecutionOutput, error) {
			assert.Equal([]string{invalidQueryID, validQueryID}, input.QueryExecutionIds)

			return &athena.BatchGetQueryExecutionOutput{
				QueryExecutions: []types.QueryExecution{
					{
						Query:            aws.String("asdas"),
						QueryExecutionId: aws.String(invalidQueryID),
						Status: &types.QueryExecutionStatus{
							SubmissionDateTime: aws.Time(time.Now().UTC()),
						},
					},
					{
						Query:            aws.String(`SELECT service_name, operation_name, span_kind FROM "jaeger" WHERE`),
						QueryExecutionId: aws.String(validQueryID),
						Status: &types.QueryExecutionStatus{
							SubmissionDateTime: aws.Time(time.Now().UTC()),
							CompletionDateTime: aws.Time(time.Now().UTC()),
						},
					},
				},
			}, nil
		})

	cache := NewTestAthenaQueryCache(mockSvc)

	cachedQuery, err := cache.Lookup(ctx, "service_name, operation_name", time.Second*60)

	assert.NoError(err)
	assert.NotNil(cachedQuery)
	assert.Equal(validQueryID, *cachedQuery.QueryExecutionId)
}

func TestOnePendingResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	validQueryID := "get-services"
	validPreviousQueryID := "get-services-old"
	invalidQueryID := "different"

	assert := assert.New(t)
	ctx := context.TODO()

	mockSvc := mocks.NewMockAthenaAPI(ctrl)
	mockSvc.EXPECT().ListQueryExecutions(gomock.Any(), gomock.Any()).
		Return(&athena.ListQueryExecutionsOutput{
			QueryExecutionIds: []string{invalidQueryID, validQueryID, validPreviousQueryID},
		}, nil)

	mockSvc.EXPECT().BatchGetQueryExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *athena.BatchGetQueryExecutionInput, _ ...func(*athena.Options)) (*athena.BatchGetQueryExecutionOutput, error) {
			assert.Equal([]string{invalidQueryID, validQueryID, validPreviousQueryID}, input.QueryExecutionIds)

			return &athena.BatchGetQueryExecutionOutput{
				QueryExecutions: []types.QueryExecution{
					{
						Query:            aws.String("asdas"),
						QueryExecutionId: aws.String(invalidQueryID),
						Status: &types.QueryExecutionStatus{
							SubmissionDateTime: aws.Time(time.Now().UTC()),
						},
					},
					{
						Query:            aws.String(`SELECT service_name, operation_name, span_kind FROM "jaeger" WHERE`),
						QueryExecutionId: aws.String(validQueryID),
						Status: &types.QueryExecutionStatus{
							CompletionDateTime: nil,
							SubmissionDateTime: aws.Time(time.Now().UTC()),
						},
					},
					{
						Query:            aws.String(`SELECT service_name, operation_name, span_kind FROM "jaeger" WHERE`),
						QueryExecutionId: aws.String(validPreviousQueryID),
						Status: &types.QueryExecutionStatus{
							CompletionDateTime: aws.Time(time.Now().UTC()),
							SubmissionDateTime: aws.Time(time.Now().UTC().Add(-10 * time.Second)),
						},
					},
				},
			}, nil
		})

	cache := NewTestAthenaQueryCache(mockSvc)

	cachedQuery, err := cache.Lookup(ctx, "service_name, operation_name", time.Second*60)

	assert.NoError(err)
	assert.NotNil(cachedQuery)
	assert.Equal(validQueryID, *cachedQuery.QueryExecutionId)
}

func TestOneStaleResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	expiredQueryID := "get-services"

	assert := assert.New(t)
	ctx := context.TODO()

	mockSvc := mocks.NewMockAthenaAPI(ctrl)
	mockSvc.EXPECT().ListQueryExecutions(gomock.Any(), gomock.Any()).
		Return(&athena.ListQueryExecutionsOutput{
			QueryExecutionIds: []string{expiredQueryID},
		}, nil)

	mockSvc.EXPECT().BatchGetQueryExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *athena.BatchGetQueryExecutionInput, _ ...func(*athena.Options)) (*athena.BatchGetQueryExecutionOutput, error) {
			assert.Equal([]string{expiredQueryID}, input.QueryExecutionIds)

			return &athena.BatchGetQueryExecutionOutput{
				QueryExecutions: []types.QueryExecution{
					{
						Query:            aws.String(`SELECT service_name, operation_name, span_kind FROM "jaeger" WHERE`),
						QueryExecutionId: aws.String(expiredQueryID),
						Status: &types.QueryExecutionStatus{
							CompletionDateTime: nil,
							SubmissionDateTime: aws.Time(time.Now().UTC().Add(-time.Second * 90)),
						},
					},
				},
			}, nil
		})

	cache := NewTestAthenaQueryCache(mockSvc)

	cachedQuery, err := cache.Lookup(ctx, "service_name, operation_name", time.Second*60)

	assert.NoError(err)
	assert.Nil(cachedQuery)
}

func TestEarlyExitWithMultiplePages(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	queryID := "get-services"

	assert := assert.New(t)
	ctx := context.TODO()

	first := true

	mockSvc := mocks.NewMockAthenaAPI(ctrl)
	mockSvc.EXPECT().ListQueryExecutions(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *athena.ListQueryExecutionsInput, _ ...func(*athena.Options)) (*athena.ListQueryExecutionsOutput, error) {
			time.Sleep(time.Millisecond * 200)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			if first {
				first = false
				return &athena.ListQueryExecutionsOutput{
					NextToken:         aws.String("next"),
					QueryExecutionIds: []string{queryID},
				}, nil
			} else {
				return &athena.ListQueryExecutionsOutput{
					QueryExecutionIds: []string{},
				}, nil
			}
		}).Times(2)

	mockSvc.EXPECT().BatchGetQueryExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *athena.BatchGetQueryExecutionInput, _ ...func(*athena.Options)) (*athena.BatchGetQueryExecutionOutput, error) {
			assert.Equal([]string{queryID}, input.QueryExecutionIds)

			return &athena.BatchGetQueryExecutionOutput{
				QueryExecutions: []types.QueryExecution{
					{
						Query:            aws.String(`SELECT service_name, operation_name, span_kind FROM "jaeger" WHERE`),
						QueryExecutionId: aws.String(queryID),
						Status: &types.QueryExecutionStatus{
							CompletionDateTime: nil,
							SubmissionDateTime: aws.Time(time.Now().UTC()),
						},
					},
				},
			}, nil
		})

	cache := NewTestAthenaQueryCache(mockSvc)

	cachedQuery, err := cache.Lookup(ctx, "service_name, operation_name", time.Second*60)

	assert.NoError(err)
	assert.NotNil(cachedQuery)
}

func TestUnprocessedResultsInBatchGetQueryExecutionResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	queryID := "get-services"

	assert := assert.New(t)
	ctx := context.TODO()

	mockSvc := mocks.NewMockAthenaAPI(ctrl)
	mockSvc.EXPECT().ListQueryExecutions(gomock.Any(), gomock.Any()).
		Return(&athena.ListQueryExecutionsOutput{
			QueryExecutionIds: []string{queryID},
		}, nil)

	mockSvc.EXPECT().BatchGetQueryExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *athena.BatchGetQueryExecutionInput, _ ...func(*athena.Options)) (*athena.BatchGetQueryExecutionOutput, error) {
			assert.Equal([]string{queryID}, input.QueryExecutionIds)

			return &athena.BatchGetQueryExecutionOutput{
				UnprocessedQueryExecutionIds: []types.UnprocessedQueryExecutionId{{
					QueryExecutionId: aws.String(queryID),
				}},
			}, nil
		})

	cache := NewTestAthenaQueryCache(mockSvc)

	cachedQuery, err := cache.Lookup(ctx, "service_name, operation_name", time.Second*60)

	assert.Error(err)
	assert.Nil(cachedQuery)
}
