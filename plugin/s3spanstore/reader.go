package s3spanstore

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
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/johanneswuerbach/jaeger-s3/plugin/config"
	"github.com/opentracing/opentracing-go"
)

// mockgen -destination=./plugin/s3spanstore/mocks/mock_athena.go -package=mocks github.com/johanneswuerbach/jaeger-s3/plugin/s3spanstore AthenaAPI

type AthenaAPI interface {
	BatchGetQueryExecution(ctx context.Context, params *athena.BatchGetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.BatchGetQueryExecutionOutput, error)
	GetQueryExecution(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error)
	GetQueryResults(ctx context.Context, params *athena.GetQueryResultsInput, optFns ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error)
	ListQueryExecutions(ctx context.Context, params *athena.ListQueryExecutionsInput, optFns ...func(*athena.Options)) (*athena.ListQueryExecutionsOutput, error)
	StartQueryExecution(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error)
	StopQueryExecution(ctx context.Context, params *athena.StopQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StopQueryExecutionOutput, error)
}

var (
	defaultMaxTraceDuration     = time.Hour * 24
	defaultDependenciesQueryTTL = time.Hour * 24
	defaultServicesQueryTtl     = time.Second * 60
)

func NewReader(ctx context.Context, logger hclog.Logger, svc AthenaAPI, cfg config.Athena) (*Reader, error) {
	maxSpanAge, err := time.ParseDuration(cfg.MaxSpanAge)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max timeframe: %w", err)
	}

	dependenciesQueryTTL, err := parseDurationWithDefault(cfg.DependenciesQueryTTL, defaultDependenciesQueryTTL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dependencies query ttl: %w", err)
	}

	servicesQueryTTL, err := parseDurationWithDefault(cfg.ServicesQueryTTL, defaultServicesQueryTtl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse services query ttl: %w", err)
	}

	maxTraceDuration, err := parseDurationWithDefault(cfg.MaxTraceDuration, defaultMaxTraceDuration)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max trace duration: %w", err)
	}

	reader := &Reader{
		svc:                  svc,
		cfg:                  cfg,
		logger:               logger,
		maxSpanAge:           maxSpanAge,
		dependenciesQueryTTL: dependenciesQueryTTL,
		servicesQueryTTL:     servicesQueryTTL,
		athenaQueryCache:     NewAthenaQueryCache(logger, svc, cfg.WorkGroup),
		maxTraceDuration:     maxTraceDuration,
	}

	reader.dependenciesPrefetch = NewDependenciesPrefetch(ctx, logger, reader, dependenciesQueryTTL, cfg.DependenciesPrefetch)
	reader.dependenciesPrefetch.Start()

	return reader, nil
}

type Reader struct {
	logger               hclog.Logger
	svc                  AthenaAPI
	cfg                  config.Athena
	maxSpanAge           time.Duration
	dependenciesQueryTTL time.Duration
	servicesQueryTTL     time.Duration
	athenaQueryCache     *AthenaQueryCache
	dependenciesPrefetch *DependenciesPrefetch
	maxTraceDuration     time.Duration
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
		`trace_id = ?`,
	}
	parameters := []string{traceID.String()}

	result, err := s.queryAthena(ctx, fmt.Sprintf(`SELECT DISTINCT span_payload FROM "%s" WHERE %s`, s.cfg.SpansTableName, strings.Join(conditions, " AND ")), parameters)
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

	result, err := s.getServicesAndOperations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query services and operations: %w", err)
	}

	serviceNameMap := map[string]bool{}
	for _, v := range result {
		serviceName := *v.Data[0].VarCharValue
		if !serviceNameMap[serviceName] {
			serviceNameMap[serviceName] = true
		}
	}

	serviceNames := make([]string, 0, len(serviceNameMap))
	for serviceName := range serviceNameMap {
		serviceNames = append(serviceNames, serviceName)
	}

	return serviceNames, nil
}

func (s *Reader) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	s.logger.Trace("GetOperations", query)
	span, _ := opentracing.StartSpanFromContext(ctx, "GetOperations")
	defer span.Finish()

	result, err := s.getServicesAndOperations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query services and operations: %w", err)
	}

	operations := []spanstore.Operation{}
	for _, v := range result {
		if query.ServiceName != *v.Data[0].VarCharValue {
			continue
		}

		if query.SpanKind != "" && query.SpanKind != *v.Data[2].VarCharValue {
			continue
		}

		operations = append(operations, spanstore.Operation{
			Name:     *v.Data[1].VarCharValue,
			SpanKind: *v.Data[2].VarCharValue,
		})
	}

	return operations, nil
}

func (r *Reader) getServicesAndOperations(ctx context.Context) ([]types.Row, error) {
	conditions := []string{
		fmt.Sprintf(`datehour BETWEEN '%s' AND '%s'`, r.DefaultMinTime().Format(PARTION_FORMAT), r.DefaultMaxTime().Format(PARTION_FORMAT)),
	}

	result, err := r.queryAthenaCached(
		ctx,
		fmt.Sprintf(`SELECT service_name, operation_name, span_kind FROM "%s" WHERE %s GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`, r.cfg.OperationsTableName, strings.Join(conditions, " AND ")),
		nil,
		fmt.Sprintf(`SELECT service_name, operation_name, span_kind FROM "%s" WHERE`, r.cfg.OperationsTableName),
		r.servicesQueryTTL)
	if err != nil {
		return nil, fmt.Errorf("failed to query athena: %w", err)
	}

	return result, nil
}

func (r *Reader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	r.logger.Trace("FindTraces", query)
	span, _ := opentracing.StartSpanFromContext(ctx, "FindTraces")
	defer span.Finish()

	// Fetch matching trace ids
	traceIDs, err := r.findTraceIDs(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query trace ids: %w", err)
	}

	if query.StartTimeMin.IsZero() {
		query.StartTimeMin = r.DefaultMinTime()
	}

	if query.StartTimeMax.IsZero() {
		query.StartTimeMax = r.DefaultMaxTime()
	}

	// Fetch span details, but only look into partitions +/- maxTraceDurations
	spanConditions := []string{
		fmt.Sprintf(`datehour BETWEEN '%s' AND '%s'`, query.StartTimeMin.Add(-r.maxTraceDuration).Format(PARTION_FORMAT), query.StartTimeMax.Add(r.maxTraceDuration).Format(PARTION_FORMAT)),
		fmt.Sprintf(`trace_id IN ('%s')`, strings.Join(traceIDs, `', '`)),
	}
	// Still use a string here as Athena only supports up to 25 parameters, which is fine as the IDs are returned by the query before.

	spanResult, err := r.queryAthena(ctx, fmt.Sprintf(`SELECT DISTINCT trace_id, span_payload FROM "%s" WHERE %s`, r.cfg.SpansTableName, strings.Join(spanConditions, " AND ")), nil)
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

func (r *Reader) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	r.logger.Trace("FindTraceIDs", query)
	span, _ := opentracing.StartSpanFromContext(ctx, "FindTraceIDs")
	defer span.Finish()

	traceIDStrings, err := r.findTraceIDs(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query trace ids: %w", err)
	}

	if traceIDStrings == nil {
		return nil, nil
	}

	traceIDs := make([]model.TraceID, len(traceIDStrings))
	for i, v := range traceIDStrings {
		traceID, err := model.TraceIDFromString(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert trace id: %w", err)
		}

		traceIDs[i] = traceID
	}

	return traceIDs, nil
}

func (r *Reader) findTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]string, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "findTraceIDs")
	defer span.Finish()

	conditions := []string{`service_name = ?`}
	parameters := []string{query.ServiceName}

	if query.OperationName != "" {
		conditions = append(conditions, `operation_name = ?`)
		parameters = append(parameters, query.OperationName)
	}

	for key, value := range query.Tags {
		conditions = append(conditions, `tags[?] = ?`)
		parameters = append(parameters, key, value)
	}

	if query.StartTimeMin.IsZero() {
		query.StartTimeMin = r.DefaultMinTime()
	}

	if query.StartTimeMax.IsZero() {
		query.StartTimeMax = r.DefaultMaxTime()
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
	result, err := r.queryAthena(ctx, fmt.Sprintf(`SELECT trace_id FROM "%s" WHERE %s GROUP BY 1 LIMIT %d`, r.cfg.SpansTableName, strings.Join(conditions, " AND "), query.NumTraces), parameters)
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

	return traceIds, nil
}

func (r *Reader) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	r.logger.Debug("GetDependencies")
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "GetDependencies")
	defer otSpan.Finish()

	startTs := endTs.Add(-lookback)

	conditions := []string{
		fmt.Sprintf(`datehour BETWEEN '%s' AND '%s'`, startTs.Format(PARTION_FORMAT), endTs.Format(PARTION_FORMAT)),
	}

	result, err := r.queryAthenaCached(ctx, fmt.Sprintf(`
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
	`, r.cfg.SpansTableName, r.cfg.SpansTableName, strings.Join(conditions, " AND ")), nil, "WITH spans_with_reference", r.dependenciesQueryTTL)
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

func (r *Reader) queryAthenaCached(ctx context.Context, queryString string, parameters []string, lookupString string, ttl time.Duration) ([]types.Row, error) {
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "queryAthenaCached")
	defer otSpan.Finish()

	queryExecution, err := r.athenaQueryCache.Lookup(ctx, lookupString, ttl)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup cached athena query: %w", err)
	}

	if queryExecution != nil {
		return r.waitAndFetchQueryResult(ctx, queryExecution)
	}

	return r.queryAthena(ctx, queryString, parameters)
}

func (r *Reader) queryAthena(ctx context.Context, queryString string, parameters []string) ([]types.Row, error) {
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "queryAthena")
	defer otSpan.Finish()

	output, err := r.svc.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString: &queryString,
		QueryExecutionContext: &types.QueryExecutionContext{
			Database: &r.cfg.DatabaseName,
		},
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: &r.cfg.OutputLocation,
		},
		ExecutionParameters: parameters,
		WorkGroup:           &r.cfg.WorkGroup,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to start athena query: %w", err)
	}

	status, err := r.svc.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
		QueryExecutionId: output.QueryExecutionId,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get athena query execution: %w", err)
	}

	return r.waitAndFetchQueryResult(ctx, status.QueryExecution)
}

func (r *Reader) waitAndFetchQueryResult(ctx context.Context, queryExecution *types.QueryExecution) ([]types.Row, error) {
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "waitAndFetchQueryResult")
	defer otSpan.Finish()

	// Poll until the query completed
	for {
		if queryExecution.Status.CompletionDateTime != nil {
			break
		}

		time.Sleep(100 * time.Millisecond)

		status, err := r.svc.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: queryExecution.QueryExecutionId,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get athena query execution: %w", err)
		}

		queryExecution = status.QueryExecution
	}

	return r.fetchQueryResult(ctx, queryExecution.QueryExecutionId)
}

func (r *Reader) fetchQueryResult(ctx context.Context, queryExecutionId *string) ([]types.Row, error) {
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "fetchQueryResult")
	defer otSpan.Finish()

	// Get query results
	paginator := athena.NewGetQueryResultsPaginator(r.svc, &athena.GetQueryResultsInput{
		QueryExecutionId: queryExecutionId,
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

func (r *Reader) Close() error {
	r.dependenciesPrefetch.Stop()
	return nil
}
