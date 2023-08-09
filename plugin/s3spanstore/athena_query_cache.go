package s3spanstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/hashicorp/go-hclog"
	"golang.org/x/sync/errgroup"
)

type AthenaQueryCache struct {
	logger    hclog.Logger
	svc       AthenaAPI
	workGroup string
}

func NewAthenaQueryCache(logger hclog.Logger, svc AthenaAPI, workGroup string) *AthenaQueryCache {
	return &AthenaQueryCache{logger: logger, svc: svc, workGroup: workGroup}
}

func (c *AthenaQueryCache) Lookup(ctx context.Context, key string, ttl time.Duration) (*types.QueryExecution, error) {
	ttlTime := time.Now().Add(-ttl)
	queryExecutionIdChunks := make(chan []string, 3)

	g, gCtx := errgroup.WithContext(ctx)
	fetchCtx, fetchCancelFunc := context.WithCancel(gCtx)

	// Page fetcher
	g.Go(func() error {
		paginator := athena.NewListQueryExecutionsPaginator(c.svc, &athena.ListQueryExecutionsInput{
			WorkGroup:  &c.workGroup,
			MaxResults: aws.Int32(50),
		})

		pages := 0
		earlyExit := false
		defer close(queryExecutionIdChunks)

	Pages:
		for paginator.HasMorePages() {
			pages += 1
			output, err := paginator.NextPage(fetchCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					earlyExit = true
					break Pages
				}

				return fmt.Errorf("failed to get athena query result: %w", err)
			}

			select {
			case <-fetchCtx.Done():
				earlyExit = true
				break Pages
			case queryExecutionIdChunks <- output.QueryExecutionIds:
			}
		}

		c.logger.Debug("AthenaQueryCache/ListQueryExecutions finished", "pages", pages, "earlyExit", earlyExit)

		return nil
	})

	// QueryExecutions lookup worker
	var latestQueryExecution *types.QueryExecution
	g.Go(func() error {
		executionsFetched := 0
		found := false

		select {
		case <-fetchCtx.Done():
			break
		case queryExecutionIds := <-queryExecutionIdChunks:
			if len(queryExecutionIds) == 0 {
				fetchCancelFunc() // Cancel search as results are ordered, so this is the most recent
				break
			}

			result, err := c.svc.BatchGetQueryExecution(gCtx, &athena.BatchGetQueryExecutionInput{
				QueryExecutionIds: queryExecutionIds,
			})
			if err != nil {
				return fmt.Errorf("failed to get query executions: %w", err)
			}

			if len(result.UnprocessedQueryExecutionIds) > 0 {
				// Likely permissions issue, so we should log and bail
				c.logger.Warn("AthenaQueryCache/BatchGetQueryExecution: unprocessed query executions", "first", result.UnprocessedQueryExecutionIds[0])
				return fmt.Errorf("failed to get query executions: unprocessed query executions")
			}

			executionsFetched += len(result.QueryExecutions)
			for _, v := range result.QueryExecutions {

				// Query already expired
				if v.Status.SubmissionDateTime.Before(ttlTime) {
					fetchCancelFunc() // Cancel search as results are ordered so no more recent query wll follow
					continue
				}

				// We don't want to match unsuccessful executions
				if v.Status.State == "FAILED" || v.Status.State == "CANCELLED" {
					continue
				}

				// Matching query
				if strings.Contains(*v.Query, key) {
					found = true
					latestQueryExecution = &v
					fetchCancelFunc() // Cancel search as results are ordered, so this is the most recent
					break
				}
			}
		}

		c.logger.Debug("AthenaQueryCache/BatchGetQueryExecution finished", "executionsFetched", executionsFetched, "found", found)

		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return latestQueryExecution, nil
}
