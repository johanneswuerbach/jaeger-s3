package s3spanstore

import (
	"context"
	"errors"
	"fmt"
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
	return &Reader{
		svc:    svc,
		cfg:    cfg,
		logger: logger,
	}, nil
}

type Reader struct {
	logger hclog.Logger
	svc    *athena.Client
	cfg    config.Athena
}

const (
	ATHENA_TIMEFORMAT = "2006-01-02 15:04:05.999"
)

func (s *Reader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	s.logger.Trace("GetTrace", traceID.String())
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "GetTrace")
	defer otSpan.Finish()

	result, err := s.queryAthena(ctx, fmt.Sprintf(`SELECT DISTINCT span_payload FROM "%s" WHERE trace_id = '%s'`, s.cfg.TableName, traceID))
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

	result, err := s.queryAthena(ctx, fmt.Sprintf(`SELECT service_name FROM "%s" GROUP BY 1 ORDER BY 1`, s.cfg.TableName))
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
	conditions := []string{fmt.Sprintf(`service_name = '%s'`, query.ServiceName)}

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

	if !query.StartTimeMin.IsZero() && !query.StartTimeMax.IsZero() {
		// conditions = append(conditions, fmt.Sprintf(`datehour BETWEEN '%s' AND '%s'`, query.StartTimeMin.Format(PARTION_FORMAT), query.StartTimeMax.Format(PARTION_FORMAT)))
		conditions = append(conditions, fmt.Sprintf(`start_time BETWEEN timestamp '%s' AND timestamp '%s'`, query.StartTimeMin.Format(ATHENA_TIMEFORMAT), query.StartTimeMax.Format(ATHENA_TIMEFORMAT)))
	} else if !query.StartTimeMin.IsZero() {
		// conditions = append(conditions, fmt.Sprintf(`datehour >= '%s'`, query.StartTimeMin.Format(PARTION_FORMAT)))
		conditions = append(conditions, fmt.Sprintf(`start_time >= timestamp '%s'`, query.StartTimeMin.Format(ATHENA_TIMEFORMAT)))
	} else if !query.StartTimeMax.IsZero() {
		// conditions = append(conditions, fmt.Sprintf(`datehour <= '%s'`, query.StartTimeMax.Format(PARTION_FORMAT)))
		conditions = append(conditions, fmt.Sprintf(`start_time <= timestamp '%s'`, query.StartTimeMax.Format(ATHENA_TIMEFORMAT)))
	}

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
	spanResult, err := s.queryAthena(ctx, fmt.Sprintf(`SELECT DISTINCT trace_id, span_payload FROM "%s" WHERE trace_id IN ('%s')`, s.cfg.TableName, strings.Join(traceIds, `', '`)))
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
