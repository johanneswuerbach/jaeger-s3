package s3spanstore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"golang.org/x/sync/errgroup"
)

type AthenaQueryCache struct {
	svc       AthenaAPI
	workGroup string
}

func NewAthenaQueryCache(svc AthenaAPI, workGroup string) *AthenaQueryCache {
	return &AthenaQueryCache{svc: svc, workGroup: workGroup}
}

func (c *AthenaQueryCache) Lookup(ctx context.Context, key string, ttl time.Duration) (*types.QueryExecution, error) {
	paginator := athena.NewListQueryExecutionsPaginator(c.svc, &athena.ListQueryExecutionsInput{
		WorkGroup: &c.workGroup,
	})
	queryExecutionIds := []string{}
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get athena query result: %w", err)
		}

		queryExecutionIds = append(queryExecutionIds, output.QueryExecutionIds...)
	}

	queryExecutionIdChunks := chunks(queryExecutionIds, 50)
	g, getQueryExecutionCtx := errgroup.WithContext(ctx)

	ttlTime := time.Now().Add(-ttl)
	var mu sync.Mutex

	var latestQueryExecution *types.QueryExecution

	for _, value := range queryExecutionIdChunks {
		value := value
		g.Go(func() error {
			result, err := c.svc.BatchGetQueryExecution(getQueryExecutionCtx, &athena.BatchGetQueryExecutionInput{
				QueryExecutionIds: value,
			})
			if err != nil {
				return err
			}

			for _, v := range result.QueryExecutions {
				// Different query
				if !strings.Contains(*v.Query, key) {
					continue
				}

				// Query didn't completed
				if v.Status.CompletionDateTime == nil {
					continue
				}

				// Query already expired
				if v.Status.CompletionDateTime.Before(ttlTime) {
					continue
				}

				mu.Lock()

				// Store the latest query result
				if latestQueryExecution == nil {
					latestQueryExecution = &v
				} else {
					if v.Status.CompletionDateTime.After(*latestQueryExecution.Status.CompletionDateTime) {
						latestQueryExecution = &v
					}
				}

				mu.Unlock()
			}

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return latestQueryExecution, nil
}
