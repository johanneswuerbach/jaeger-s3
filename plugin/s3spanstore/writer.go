package s3spanstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/johanneswuerbach/jaeger-s3/plugin/config"
)

type KinesisAPI interface {
	PutRecordBatch(ctx context.Context, params *firehose.PutRecordBatchInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error)
}

const (
	MAX_BATCH_BYTE_SIZE = 1024 * 1024 * 4
	MAX_BATCH_RECORDS   = 500
)

var (
	MIN_FLUSH_INTERVAL = 10 * time.Second
)

func NewWriter(logger hclog.Logger, svc KinesisAPI, firehoseConfig config.Kinesis) (*Writer, error) {
	w := &Writer{
		svc:             svc,
		spansStreamName: firehoseConfig.SpanStreamName,
		logger:          logger,
		ticker:          time.NewTicker(MIN_FLUSH_INTERVAL),
		done:            make(chan bool),
	}

	ctx := context.Background()
	w.emptyRecordsBuffer()

	go func() {
		for {
			select {
			case <-w.done:
				return
			case <-w.ticker.C:
				if time.Since(w.lastFlush) > MIN_FLUSH_INTERVAL {
					if err := w.flushBuffer(ctx); err != nil {
						w.logger.Error("failed to flush buffer after min flush interval", err)
					}
				}
			}
		}
	}()

	return w, nil
}

type Writer struct {
	logger          hclog.Logger
	svc             KinesisAPI
	spansStreamName string
	ticker          *time.Ticker
	done            chan bool

	lastFlush     time.Time
	recordsLength int
	recordsBuffer []types.Record
}

// SpanRecord contains queryable properties from the span and the span as json payload
type SpanRecord struct {
	TraceID       string                  `json:"traceid"`       // 1
	SpanID        string                  `json:"spanid"`        // 2
	OperationName string                  `json:"operationname"` // 3
	SpanKind      string                  `json:"spankind"`      // 4
	StartTime     int64                   `json:"starttime"`     // 5
	Duration      int64                   `json:"duration"`      // 6
	Tags          map[string]string       `json:"tags"`          // 7
	ServiceName   string                  `json:"servicename"`   // 8
	SpanPayload   string                  `json:"spanpayload"`   // 9
	References    []*SpanRecordReferences `json:"references"`    // 10
}

type SpanRecordReferences struct {
	TraceID string `json:"traceid"` // 1
	SpanID  string `json:"spanid"`  // 2
	RefType int64  `json:"reftype"` // 3
}

func NewSpanRecordReferencesFromSpanReferences(span *model.Span) []*SpanRecordReferences {
	spanRecordReferences := make([]*SpanRecordReferences, len(span.References))

	for i, v := range span.References {
		spanRecordReferences[i] = &SpanRecordReferences{
			TraceID: v.TraceID.String(),
			SpanID:  v.SpanID.String(),
			RefType: int64(v.RefType),
		}
	}

	return spanRecordReferences
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
		References:    NewSpanRecordReferencesFromSpanReferences(span),
	}, nil
}

func kvToMap(kvs []model.KeyValue) map[string]string {
	kvMap := map[string]string{}
	for _, field := range kvs {
		kvMap[field.Key] = field.AsString()
	}

	return kvMap
}

func spanToRecord(span *model.Span) ([]byte, error) {
	spanRecord, err := NewSpanRecordFromSpan(span)
	if err != nil {
		return nil, fmt.Errorf("failed to create span record: %w", err)
	}
	spanRecordBytes, err := json.Marshal(spanRecord)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize span record: %w", err)
	}

	return spanRecordBytes, nil
}

func (s *Writer) emptyRecordsBuffer() []types.Record {
	recordsBuffer := s.recordsBuffer
	s.recordsBuffer = make([]types.Record, 0)
	s.recordsLength = 0
	s.lastFlush = time.Now()

	return recordsBuffer
}

func (s *Writer) addRecordToBuffer(record []byte, recordLength int) int {
	s.recordsBuffer = append(s.recordsBuffer, types.Record{
		Data: record,
	})
	s.recordsLength += recordLength

	return len(s.recordsBuffer)
}

func (s *Writer) flushBuffer(ctx context.Context) error {
	recordsBuffer := s.emptyRecordsBuffer()

	s.logger.Debug("flushBuffer", "records", len(recordsBuffer))

	_, err := s.svc.PutRecordBatch(ctx, &firehose.PutRecordBatchInput{
		DeliveryStreamName: &s.spansStreamName,
		Records:            recordsBuffer,
	})
	if err != nil {
		return fmt.Errorf("failed to put item: %w", err)
	}
	return nil
}

func (s *Writer) writeSpanItem(ctx context.Context, span *model.Span) error {
	spanRecordBytes, err := spanToRecord(span)
	if err != nil {
		return fmt.Errorf("failed to convert span to record: %w", err)
	}
	spanRecordLength := len(spanRecordBytes)

	if s.recordsLength+int(spanRecordLength) > MAX_BATCH_BYTE_SIZE {
		if err := s.flushBuffer(ctx); err != nil {
			return fmt.Errorf("failed to flush buffer after max byte size: %w", err)
		}
	}

	if s.addRecordToBuffer(spanRecordBytes, spanRecordLength) >= MAX_BATCH_RECORDS {
		if err := s.flushBuffer(ctx); err != nil {
			return fmt.Errorf("failed to flush buffer after max records: %w", err)
		}
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

func (w *Writer) Close() error {
	w.ticker.Stop()
	w.done <- true

	ctx := context.Background()

	return w.flushBuffer(ctx)
}
