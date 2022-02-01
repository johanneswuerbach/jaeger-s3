package s3spanstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
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

func toSpan(payload string) (*model.Span, error) {
	payloadBytes, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decode payload: %w", err)
	}

	b := bytes.NewBuffer(payloadBytes)
	r := snappy.NewReader(b)

	var resB bytes.Buffer
	_, err = resB.ReadFrom(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress payload: %w", err)
	}

	span := &model.Span{}
	if err := proto.Unmarshal(resB.Bytes(), span); err != nil {
		return nil, fmt.Errorf("failed to unmarshal span: %w", err)
	}
	return span, nil
}

func (s *Reader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	s.logger.Trace("GetTrace", traceID.String())
	otSpan, _ := opentracing.StartSpanFromContext(ctx, "GetTrace")
	defer otSpan.Finish()

	result, err := s.queryAthena(ctx, fmt.Sprintf(`SELECT span_payload FROM "%s" WHERE trace_id = '%s'`, s.cfg.TableName, traceID))
	if err != nil {
		return nil, fmt.Errorf("failed to query athena: %w", err)
	}
	if len(result) == 0 {
		return nil, spanstore.ErrTraceNotFound
	}

	spans := make([]*model.Span, len(result))
	for i, v := range result {
		span, err := toSpan(*v.Data[0].VarCharValue)
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

	conditions := fmt.Sprintf(`service_name = '%s'`, query.ServiceName)
	if query.SpanKind != "" {
		conditions += fmt.Sprintf(` AND span_kind = '%s'`, query.SpanKind)
	}

	result, err := s.queryAthena(ctx, fmt.Sprintf(`SELECT operation_name, span_kind FROM "%s" WHERE %s GROUP BY 1, 2 ORDER BY 1, 2`, s.cfg.TableName, conditions))
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

	conditions := fmt.Sprintf(`service_name = '%s'`, query.ServiceName)
	if query.OperationName != "" {
		conditions += fmt.Sprintf(` AND operation_name = '%s'`, query.OperationName)
	}

	// TODO Implement the other query operations
	// TODO SQL injection

	// Fetch trace ids
	result, err := s.queryAthena(ctx, fmt.Sprintf(`SELECT trace_id FROM "%s" WHERE %s GROUP BY 1 LIMIT %d`, s.cfg.TableName, conditions, query.NumTraces))
	if err != nil {
		return nil, fmt.Errorf("failed to query athena: %w", err)
	}
	if len(result) == 0 {
		return []*model.Trace{}, nil
	}

	traceIds := make([]string, len(result))
	for i, v := range result {
		traceIds[i] = *v.Data[0].VarCharValue
	}

	// Fetch span details
	spanResult, err := s.queryAthena(ctx, fmt.Sprintf(`SELECT trace_id, span_payload FROM "%s" WHERE trace_id IN ('%s')`, s.cfg.TableName, strings.Join(traceIds, `', '`)))
	if err != nil {
		return nil, fmt.Errorf("failed to query athena: %w", err)
	}

	traceIdSpans := map[string][]*model.Span{}
	for _, v := range spanResult {
		traceId := *v.Data[0].VarCharValue
		span, err := toSpan(*v.Data[1].VarCharValue)
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

		pageRows := output.ResultSet.Rows
		if len(pageRows) > 1 {
			rows = append(rows, pageRows[1:]...)
		}
	}

	return rows, nil
}
