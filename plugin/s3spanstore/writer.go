package s3spanstore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/johanneswuerbach/jaeger-s3/plugin/config"
)

type KinesisAPI interface {
	PutRecord(ctx context.Context, params *firehose.PutRecordInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordOutput, error)
}

func NewWriter(logger hclog.Logger, svc KinesisAPI, firehoseConfig config.Kinesis) (*Writer, error) {
	return &Writer{
		svc:             svc,
		spansStreamName: firehoseConfig.SpanStreamName,
		logger:          logger,
	}, nil
}

type Writer struct {
	logger          hclog.Logger
	svc             KinesisAPI
	spansStreamName string
}

// SpanRecord contains queryable properties from the span and the span as json payload
type SpanRecord struct {
	TraceID       string            `json:"traceid"`       // 1
	SpanID        string            `json:"spanid"`        // 2
	OperationName string            `json:"operationname"` // 3
	SpanKind      string            `json:"spankind"`      // 4
	StartTime     int64             `json:"starttime"`     // 5
	Duration      int64             `json:"duration"`      // 6
	Tags          map[string]string `json:"tags"`          // 7
	ServiceName   string            `json:"servicename"`   // 8
	SpanPayload   string            `json:"spanpayload"`   // 9
}

func NewSpanRecordFromSpan(span *model.Span) (*SpanRecord, error) {
	searchableTags := append([]model.KeyValue{}, span.Tags...)
	searchableTags = append(searchableTags, span.Process.Tags...)
	for _, log := range span.Logs {
		searchableTags = append(searchableTags, log.Fields...)
	}

	spanBytes, err := json.Marshal(span)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize item: %w", err)
	}

	kind, _ := span.GetSpanKind()

	return &SpanRecord{
		TraceID:       span.TraceID.String(),
		SpanID:        span.SpanID.String(),
		OperationName: span.OperationName,
		SpanKind:      kind,
		StartTime:     span.StartTime.UnixMilli(),
		Duration:      span.Duration.Nanoseconds(),
		Tags:          kvToMap(searchableTags),
		ServiceName:   span.Process.ServiceName,
		SpanPayload:   string(spanBytes),
	}, nil
}

func kvToMap(kvs []model.KeyValue) map[string]string {
	kvMap := map[string]string{}
	for _, field := range kvs {
		kvMap[field.Key] = field.AsString()
	}

	return kvMap
}

func (s *Writer) writeSpanItem(ctx context.Context, span *model.Span) error {
	spanRecord, err := NewSpanRecordFromSpan(span)
	if err != nil {
		return fmt.Errorf("failed to create span record: %w", err)
	}
	spanRecordBytes, err := json.Marshal(spanRecord)
	if err != nil {
		return fmt.Errorf("failed to serialize span record: %w", err)
	}

	_, err = s.svc.PutRecord(ctx, &firehose.PutRecordInput{
		Record: &types.Record{
			Data: spanRecordBytes,
		},
		DeliveryStreamName: &s.spansStreamName,
	})
	if err != nil {
		return fmt.Errorf("failed to put item: %w", err)
	}

	// s.logger.Debug("PutRecord", out)

	return nil
}

func (s *Writer) WriteSpan(ctx context.Context, span *model.Span) error {
	// s.logger.Debug("WriteSpan", span)

	if err := s.writeSpanItem(ctx, span); err != nil {
		return fmt.Errorf("failed to write span item, %v", err)
	}
	return nil
}
