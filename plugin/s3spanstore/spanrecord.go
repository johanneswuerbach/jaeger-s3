package s3spanstore

import (
	"bytes"
	"encoding/base64"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/jaegertracing/jaeger/model"
)

// SpanRecord contains queryable properties from the span and the span as json payload
type SpanRecord struct {
	TraceID       string             `parquet:"name=trace_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	SpanID        string             `parquet:"name=span_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	OperationName string             `parquet:"name=operation_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SpanKind      string             `parquet:"name=span_kind, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	StartTime     int64              `parquet:"name=start_time, type=INT64"`
	Duration      int64              `parquet:"name=duration, type=INT64"`
	Tags          *map[string]string `parquet:"name=tags, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	ServiceName   string             `parquet:"name=service_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`

	// TODO: Write binary
	SpanPayload string                   `parquet:"name=span_payload, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	References  *[]*SpanRecordReferences `parquet:"name=references"`
}

type SpanRecordReferences struct {
	TraceID string `parquet:"name=trace_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	SpanID  string `parquet:"name=span_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	RefType int64  `parquet:"name=ref_type, type=INT64"`
}

func NewSpanRecordReferencesFromSpanReferences(span *model.Span) *[]*SpanRecordReferences {
	spanRecordReferences := make([]*SpanRecordReferences, len(span.References))

	for i, v := range span.References {
		spanRecordReferences[i] = &SpanRecordReferences{
			TraceID: v.TraceID.String(),
			SpanID:  v.SpanID.String(),
			RefType: int64(v.RefType),
		}
	}

	return &spanRecordReferences
}

func EncodeSpanPayload(span *model.Span) (string, error) {
	spanBytes, err := proto.Marshal(span)
	if err != nil {
		return "", fmt.Errorf("failed to serialize item: %w", err)
	}

	var b bytes.Buffer
	b64 := base64.NewEncoder(base64.StdEncoding, &b)
	sn := snappy.NewBufferedWriter(b64)

	_, err = sn.Write(spanBytes)
	if err != nil {
		return "", fmt.Errorf("failed to write compress span: %w", err)
	}

	if err = sn.Close(); err != nil {
		return "", fmt.Errorf("failed to close compress span: %w", err)
	}

	if err = b64.Close(); err != nil {
		return "", fmt.Errorf("failed to base64 span: %w", err)
	}

	return b.String(), nil
}

func NewSpanRecordFromSpan(span *model.Span) (*SpanRecord, error) {
	searchableTags := append([]model.KeyValue{}, span.Tags...)
	searchableTags = append(searchableTags, span.Process.Tags...)
	for _, log := range span.Logs {
		searchableTags = append(searchableTags, log.Fields...)
	}

	spanPayload, err := EncodeSpanPayload(span)
	if err != nil {
		return nil, fmt.Errorf("failed to create span payload: %w", err)
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
		SpanPayload:   spanPayload,
		References:    NewSpanRecordReferencesFromSpanReferences(span),
	}, nil
}

func kvToMap(kvs []model.KeyValue) *map[string]string {
	kvMap := map[string]string{}
	for _, field := range kvs {
		kvMap[field.Key] = field.AsString()
	}

	return &kvMap
}
