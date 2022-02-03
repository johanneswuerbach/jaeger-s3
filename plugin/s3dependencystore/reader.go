package s3dependencystore

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/johanneswuerbach/jaeger-s3/plugin/config"
	"github.com/opentracing/opentracing-go"
)

func NewReader(logger hclog.Logger, svc *athena.Client, cfg config.Athena) *Reader {
	return &Reader{
		svc:    svc,
		cfg:    cfg,
		logger: logger,
	}
}

type Reader struct {
	logger hclog.Logger
	svc    *athena.Client
	cfg    config.Athena
}

const (
	ATHENA_TIMEFORMAT = "2006-01-02 15:04:05.999"
)

func (r *Reader) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	r.logger.Debug("GetDependencies")
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "GetDependencies")
	defer otSpan.Finish()

	conditions := []string{fmt.Sprintf(`start_time BETWEEN timestamp '%s' AND timestamp '%s'`, endTs.Add(-lookback).Format(ATHENA_TIMEFORMAT), endTs.Format(ATHENA_TIMEFORMAT))}
	result, err := r.queryAthena(ctx, fmt.Sprintf(`
		WITH spans_with_references AS (
			SELECT
				base.service_name,
				base.trace_id,
				base.span_id,
				unnested_references.reference.trace_id as ref_trace_id,
				unnested_references.reference.span_id as ref_span_id
			FROM %s as base
			CROSS JOIN UNNEST(jaeger.references) AS unnested_references (reference)
		)

		SELECT spans_with_references.service_name as child, jaeger.service_name as parent, COUNT(*) as callcount
			FROM spans_with_references
			JOIN %s as jaeger ON spans_with_references.ref_trace_id = jaeger.trace_id AND spans_with_references.ref_span_id = jaeger.span_id
			WHERE %s
			GROUP BY 1, 2
	`, r.cfg.TableName, r.cfg.TableName, strings.Join(conditions, " AND ")))
	if err != nil {
		return nil, fmt.Errorf("failed to query athena: %w", err)
	}

	dependencyLinks := make([]model.DependencyLink, len(result))
	for i, v := range result {
		callCount, err := strconv.ParseUint(*v.Data[2].VarCharValue, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse call count: %w", err)
		}

		dependencyLinks[i] = model.DependencyLink{
			Parent:    *v.Data[0].VarCharValue,
			Child:     *v.Data[1].VarCharValue,
			CallCount: callCount,
		}
	}

	return dependencyLinks, nil
}

func (r *Reader) queryAthena(ctx context.Context, queryString string) ([]types.Row, error) {
	output, err := r.svc.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString: &queryString,
		QueryExecutionContext: &types.QueryExecutionContext{
			Database: &r.cfg.DatabaseName,
		},
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: &r.cfg.OutputLocation,
		},
		WorkGroup: &r.cfg.WorkGroup,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to start athena query: %w", err)
	}

	// Poll until the query completed
	for {
		status, err := r.svc.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: output.QueryExecutionId,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get athena query execution: %w", err)
		}
		if status.QueryExecution.Status.CompletionDateTime != nil {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	// Get query results
	paginator := athena.NewGetQueryResultsPaginator(r.svc, &athena.GetQueryResultsInput{
		QueryExecutionId: output.QueryExecutionId,
	})
	rows := []types.Row{}
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get athena query result: %w", err)
		}

		rows = append(rows, output.ResultSet.Rows...)
	}

	// Remove the table header
	if len(rows) >= 1 {
		rows = rows[1:]
	}

	return rows, nil
}
