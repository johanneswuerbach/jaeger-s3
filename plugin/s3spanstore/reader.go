package s3spanstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/johanneswuerbach/jaeger-s3/plugin/config"
	"github.com/opentracing/opentracing-go"
)

func NewReader(logger hclog.Logger, svc *athena.Client, cfg config.Athena) (*Reader, error) {
	maxSpanAge, err := time.ParseDuration(cfg.MaxSpanAge)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max timeframe: %w", err)
	}

	return &Reader{
		svc:        svc,
		cfg:        cfg,
		logger:     logger,
		maxSpanAge: maxSpanAge,
	}, nil
}

type Reader struct {
	logger     hclog.Logger
	svc        *athena.Client
	cfg        config.Athena
	maxSpanAge time.Duration
}

const (
	ATHENA_TIMEFORMAT = "2006-01-02 15:04:05.999"
)

func (r *Reader) DefaultMaxTime() time.Time {
	return time.Now().UTC()
}

func (r *Reader) DefaultMinTime() time.Time {
	return r.DefaultMaxTime().Add(-r.maxSpanAge)
}

func (s *Reader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	s.logger.Trace("GetTrace", traceID.String())
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "GetTrace")
	defer otSpan.Finish()

	conditions := []string{
		fmt.Sprintf(`datehour BETWEEN '%s' AND '%s'`, s.DefaultMinTime().Format(PARTION_FORMAT), s.DefaultMaxTime().Format(PARTION_FORMAT)),
		fmt.Sprintf(`trace_id = '%s'`, traceID),
	}

	result, err := s.queryAthena(ctx, fmt.Sprintf(`SELECT DISTINCT span_payload FROM "%s" WHERE %s`, s.cfg.TableName, strings.Join(conditions, " AND ")))
	if err != nil {
		return nil, fmt.Errorf("failed to query athena: %w", err)
	}
	if len(result) == 0 {
		return nil, spanstore.ErrTraceNotFound
	}

	spans := make([]*model.Span, len(result))
	for i, v := range result {
		span, err := DecodeSpanPayload(*v.Data[0].VarCharValue)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal span: %w", err)
		}
		spans[i] = span
	}

	return &model.Trace{
		Spans: spans,
	}, nil
}

func (s *Reader) GetServices(ctx context.Context) ([]string, error) {
	s.logger.Trace("GetServices")
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "GetServices")
	defer otSpan.Finish()

	conditions := []string{
		fmt.Sprintf(`datehour BETWEEN '%s' AND '%s'`, s.DefaultMinTime().Format(PARTION_FORMAT), s.DefaultMaxTime().Format(PARTION_FORMAT)),
	}

	result, err := s.queryAthena(ctx, fmt.Sprintf(`SELECT service_name FROM "%s" WHERE %s GROUP BY 1 ORDER BY 1`, s.cfg.TableName, strings.Join(conditions, " AND ")))
	if err != nil {
		return nil, fmt.Errorf("failed to query athena: %w", err)
	}

	serviceNames := make([]string, len(result))
	for i, v := range result {
		serviceNames[i] = *v.Data[0].VarCharValue
	}

	return serviceNames, nil
}

func (s *Reader) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	s.logger.Trace("GetOperations", query)
	span, _ := opentracing.StartSpanFromContext(ctx, "GetOperations")
	defer span.Finish()

	// TODO Prevent SQL injections
	conditions := []string{
		fmt.Sprintf(`datehour BETWEEN '%s' AND '%s'`, s.DefaultMinTime().Format(PARTION_FORMAT), s.DefaultMaxTime().Format(PARTION_FORMAT)),
		fmt.Sprintf(`service_name = '%s'`, query.ServiceName),
	}

	if query.SpanKind != "" {
		conditions = append(conditions, fmt.Sprintf(`span_kind = '%s'`, query.SpanKind))
	}

	result, err := s.queryAthena(ctx, fmt.Sprintf(`SELECT operation_name, span_kind FROM "%s" WHERE %s GROUP BY 1, 2 ORDER BY 1, 2`, s.cfg.TableName, strings.Join(conditions, " AND ")))
	if err != nil {
		return nil, fmt.Errorf("failed to query athena: %w", err)
	}

	operations := make([]spanstore.Operation, len(result))
	for i, v := range result {
		operations[i] = spanstore.Operation{
			Name:     *v.Data[0].VarCharValue,
			SpanKind: *v.Data[1].VarCharValue,
		}
	}

	return operations, nil
}

func (s *Reader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	s.logger.Trace("FindTraces", query)
	span, _ := opentracing.StartSpanFromContext(ctx, "FindTraces")
	defer span.Finish()

	// TODO Prevent SQL injections
	conditions := []string{fmt.Sprintf(`service_name = '%s'`, query.ServiceName)}

	if query.OperationName != "" {
		conditions = append(conditions, fmt.Sprintf(`operation_name = '%s'`, query.OperationName))
	}

	for key, value := range query.Tags {
		conditions = append(conditions, fmt.Sprintf(`tags['%s'] = '%s'`, key, value))
	}

	if query.StartTimeMin.IsZero() {
		query.StartTimeMin = s.DefaultMinTime()
	}

	if query.StartTimeMax.IsZero() {
		query.StartTimeMax = s.DefaultMaxTime()
	}

	conditions = append(conditions, fmt.Sprintf(`datehour BETWEEN '%s' AND '%s'`, query.StartTimeMin.Format(PARTION_FORMAT), query.StartTimeMax.Format(PARTION_FORMAT)))
	conditions = append(conditions, fmt.Sprintf(`start_time BETWEEN timestamp '%s' AND timestamp '%s'`, query.StartTimeMin.Format(ATHENA_TIMEFORMAT), query.StartTimeMax.Format(ATHENA_TIMEFORMAT)))

	if query.DurationMin.String() != "0s" && query.DurationMax.String() != "0s" {
		conditions = append(conditions, fmt.Sprintf(`duration BETWEEN %d AND %d`, query.DurationMin.Nanoseconds(), query.DurationMax.Nanoseconds()))
	} else if query.DurationMin.String() != "0s" {
		conditions = append(conditions, fmt.Sprintf(`duration >= %d`, query.DurationMin.Nanoseconds()))
	} else if query.DurationMax.String() != "0s" {
		conditions = append(conditions, fmt.Sprintf(`duration <= %d`, query.DurationMax.Nanoseconds()))
	}

	// Fetch trace ids
	result, err := s.queryAthena(ctx, fmt.Sprintf(`SELECT trace_id FROM "%s" WHERE %s GROUP BY 1 LIMIT %d`, s.cfg.TableName, strings.Join(conditions, " AND "), query.NumTraces))
	if err != nil {
		return nil, fmt.Errorf("failed to query athena: %w", err)
	}
	if len(result) == 0 {
		return nil, nil
	}

	traceIds := make([]string, len(result))
	for i, v := range result {
		traceIds[i] = *v.Data[0].VarCharValue
	}

	// Fetch span details
	spanConditions := []string{
		fmt.Sprintf(`datehour BETWEEN '%s' AND '%s'`, s.DefaultMinTime().Format(PARTION_FORMAT), s.DefaultMaxTime().Format(PARTION_FORMAT)),
		fmt.Sprintf(`trace_id IN ('%s')`, strings.Join(traceIds, `', '`)),
	}

	spanResult, err := s.queryAthena(ctx, fmt.Sprintf(`SELECT DISTINCT trace_id, span_payload FROM "%s" WHERE %s`, s.cfg.TableName, strings.Join(spanConditions, " AND ")))
	if err != nil {
		return nil, fmt.Errorf("failed to query athena: %w", err)
	}

	traceIdSpans := map[string][]*model.Span{}
	for _, v := range spanResult {
		traceId := *v.Data[0].VarCharValue
		span, err := DecodeSpanPayload(*v.Data[1].VarCharValue)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal span: %w", err)
		}

		spans, exists := traceIdSpans[traceId]
		if !exists {
			spans = []*model.Span{span}
		} else {
			spans = append(spans, span)
		}
		traceIdSpans[traceId] = spans
	}

	traces := []*model.Trace{}
	for _, v := range traceIdSpans {
		traces = append(traces, &model.Trace{
			Spans: v,
		})
	}

	return traces, nil
}

// This method is not used
func (s *Reader) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	s.logger.Trace("FindTraceIDs", query)
	span, _ := opentracing.StartSpanFromContext(ctx, "FindTraceIDs")
	defer span.Finish()

	return nil, errors.New("not implemented")
}

func (r *Reader) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	r.logger.Debug("GetDependencies")
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "GetDependencies")
	defer otSpan.Finish()

	startTs := endTs.Add(-lookback)

	conditions := []string{
		fmt.Sprintf(`start_time BETWEEN timestamp '%s' AND timestamp '%s'`, startTs.Format(ATHENA_TIMEFORMAT), endTs.Format(ATHENA_TIMEFORMAT)),
		fmt.Sprintf(`datehour BETWEEN '%s' AND '%s'`, startTs.Format(PARTION_FORMAT), endTs.Format(PARTION_FORMAT)),
	}

	result, err := r.queryAthena(ctx, fmt.Sprintf(`
		WITH spans_with_references AS (
			SELECT
				base.service_name,
				base.trace_id,
				base.span_id,
				unnested_references.reference.trace_id as ref_trace_id,
				unnested_references.reference.span_id as ref_span_id
			FROM %s as base
			CROSS JOIN UNNEST(base.references) AS unnested_references (reference)
		)

		SELECT jaeger.service_name as parent, spans_with_references.service_name as child, COUNT(*) as callcount
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

func (s *Reader) queryAthena(ctx context.Context, queryString string) ([]types.Row, error) {
	output, err := s.svc.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString: &queryString,
		QueryExecutionContext: &types.QueryExecutionContext{
			Database: &s.cfg.DatabaseName,
		},
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: &s.cfg.OutputLocation,
		},
		WorkGroup: &s.cfg.WorkGroup,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to start athena query: %w", err)
	}

	// Poll until the query completed
	for {
		status, err := s.svc.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
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
	paginator := athena.NewGetQueryResultsPaginator(s.svc, &athena.GetQueryResultsInput{
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
