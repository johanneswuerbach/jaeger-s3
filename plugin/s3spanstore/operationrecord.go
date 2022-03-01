package s3spanstore

import (
	"fmt"

	"github.com/jaegertracing/jaeger/model"
)

// OperationRecord contains queryable properties
type OperationRecord struct {
	OperationName string `parquet:"name=operation_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SpanKind      string `parquet:"name=span_kind, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ServiceName   string `parquet:"name=service_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func NewOperationRecordFromSpan(span *model.Span) (*OperationRecord, error) {
	kind, _ := span.GetSpanKind()

	return &OperationRecord{
		OperationName: span.OperationName,
		SpanKind:      kind,
		ServiceName:   span.Process.ServiceName,
	}, nil
}

func (w *OperationRecord) DedupeKey() string {
	return fmt.Sprintf("%s/%s/%s", w.OperationName, w.SpanKind, w.ServiceName)
}
