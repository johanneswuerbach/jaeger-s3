package s3spanstore

import (
	"context"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/golang/mock/gomock"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/johanneswuerbach/jaeger-s3/plugin/config"
	"github.com/johanneswuerbach/jaeger-s3/plugin/s3spanstore/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func NewTestWriter(ctx context.Context, assert *assert.Assertions, mockSvc *mocks.MockS3API) *Writer {
	loggerName := "jaeger-s3"

	logLevel := os.Getenv("GRPC_STORAGE_PLUGIN_LOG_LEVEL")
	if logLevel == "" {
		logLevel = hclog.Debug.String()
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.LevelFromString(logLevel),
		Name:       loggerName,
		JSONFormat: true,
	})

	writer, err := NewWriter(ctx, logger, mockSvc, config.S3{
		BucketName:       "jaeger-spans",
		SpansPrefix:      "/spans/",
		OperationsPrefix: "/operations/",
	})

	assert.NoError(err)

	return writer
}

func NewTestSpan(assert *assert.Assertions) *model.Span {
	var span model.Span
	assert.NoError(jsonpb.Unmarshal(strings.NewReader(`{
		"traceId": "AAAAAAAAAAAAAAAAAAAAEQ==",
		"spanId": "AAAAAAAAAAM=",
		"operationName": "example-operation-1",
		"references": [],
		"startTime": "2017-01-26T16:46:31.639875Z",
		"duration": "100000ns",
		"tags": [],
		"process": {
			"serviceName": "example-service-1",
			"tags": []
		},
		"logs": [
			{
				"timestamp": "2017-01-26T16:46:31.639875Z",
				"fields": []
			},
			{
				"timestamp": "2017-01-26T16:46:31.639875Z",
				"fields": []
			}
		]
	}`), &span))

	return &span
}

func NewTestSpanWithTagsAndReferences(assert *assert.Assertions) *model.Span {
	var span model.Span
	assert.NoError(jsonpb.Unmarshal(strings.NewReader(`{
		"traceId": "AAAAAAAAAAAAAAAAAAAAEg==",
		"spanId": "AAAAAAAAAAQ=",
		"operationName": "query12-operation",
		"references": [
			{
				"refType": "CHILD_OF",
				"traceId": "AAAAAAAAAAAAAAAAAAAA/w==",
				"spanId": "AAAAAAAAAP8="
			}
		],
		"tags": [
			{
				"key": "sameplacetag1",
				"vType": "STRING",
				"vStr": "sameplacevalue"
			},
			{
				"key": "sameplacetag2",
				"vType": "INT64",
				"vInt64": 123
			},
			{
				"key": "sameplacetag4",
				"vType": "BOOL",
				"vBool": true
			},
			{
				"key": "sameplacetag3",
				"vType": "FLOAT64",
				"vFloat64": 72.5
			},
			{
				"key": "blob",
				"vType": "BINARY",
				"vBinary": "AAAwOQ=="
			}
		],
		"startTime": "2017-01-26T16:46:31.639875Z",
		"duration": "2000ns",
		"process": {
			"serviceName": "query12-service",
			"tags": []
		},
		"logs": []
	}`), &span))

	return &span
}

func TestS3ParquetKey(t *testing.T) {
	assert := assert.New(t)

	testTime1 := time.Date(2021, 1, 30, 6, 34, 58, 123, time.UTC)

	assert.Equal("prefix/2021/01/30/06/random.parquet", S3ParquetKey("prefix/", "random", S3PartitionKey(testTime1)))

	testTime2 := time.Date(2021, 1, 30, 18, 34, 58, 123, time.UTC)

	assert.Equal("prefix/2021/01/30/18/random.parquet", S3ParquetKey("prefix/", "random", S3PartitionKey(testTime2)))
}

func TestWriteSpan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSvc := mocks.NewMockS3API(ctrl)

	assert := assert.New(t)
	ctx := context.TODO()

	file, err := ioutil.TempFile("", "write-span")
	assert.NoError(err)
	defer os.Remove(file.Name())

	mockSvc.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			dat, err := ioutil.ReadAll(input.Body)
			assert.NoError(err)
			assert.NoError(ioutil.WriteFile(file.Name(), dat, 0644))

			return &s3.PutObjectOutput{}, nil
		}).Times(2)

	writer := NewTestWriter(ctx, assert, mockSvc)

	span := NewTestSpan(assert)

	assert.NoError(writer.WriteSpan(ctx, span))

	assert.NoError(writer.Close())

	localFileReader, err := local.NewLocalFileReader(file.Name())
	assert.NoError(err)
	pr, err := reader.NewParquetReader(localFileReader, new(SpanRecord), 1)
	assert.NoError(err)

	num := int(pr.GetNumRows())
	assert.Equal(1, num)

	records := make([]SpanRecord, 1)
	assert.NoError(pr.Read(&records))

	record := records[0]

	assert.Equal("0000000000000011", record.TraceID)
	assert.Equal("0000000000000003", record.SpanID)
	assert.Equal("example-operation-1", record.OperationName)
	assert.Equal("", record.SpanKind)
	assert.Equal(int64(1485449191639), record.StartTime)
	assert.Equal(int64(100000), record.Duration)
	assert.Equal(map[string]string{}, record.Tags)
	assert.Equal("example-service-1", record.ServiceName)
	assert.Equal("/wYAAHNOYVBwWQBZAAB5D7oLeggKEAA2AQAIERIIDRGwAxoTZXhhbXBsZS1vcGVyYXRpb24tMTIMCOfPqMQFELjvjrECOgQQoI0GSg4KMhYAAEo6EAAMUhMKERFLIHNlcnZpY2UtMQ==", record.SpanPayload)
	assert.Equal([]SpanRecordReferences{}, record.References)

	pr.ReadStop()
	assert.NoError(localFileReader.Close())
}

type S3PutObject struct {
	key      string
	fileName string
}

func TestWriteSpanTwice(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSvc := mocks.NewMockS3API(ctrl)

	assert := assert.New(t)
	ctx := context.TODO()

	objects := []*S3PutObject{}
	defer func() {
		for _, object := range objects {
			os.Remove(object.fileName)
		}
	}()

	mockSvc.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			file, err := ioutil.TempFile("", "write-span")
			assert.NoError(err)
			objects = append(objects, &S3PutObject{
				key:      *input.Key,
				fileName: file.Name(),
			})

			dat, err := ioutil.ReadAll(input.Body)
			assert.NoError(err)
			assert.NoError(ioutil.WriteFile(file.Name(), dat, 0644))

			return &s3.PutObjectOutput{}, nil
		}).Times(2)

	writer := NewTestWriter(ctx, assert, mockSvc)

	span := NewTestSpan(assert)

	assert.NoError(writer.WriteSpan(ctx, span))
	assert.NoError(writer.WriteSpan(ctx, span))

	assert.NoError(writer.Close())

	spansFile := ""
	for _, object := range objects {
		if strings.HasPrefix(object.key, "/spans") {
			spansFile = object.fileName
		}
	}

	assert.NotEmpty(spansFile)

	localFileReader, err := local.NewLocalFileReader(spansFile)
	assert.NoError(err)
	pr, err := reader.NewParquetReader(localFileReader, new(SpanRecord), 1)
	assert.NoError(err)

	num := int(pr.GetNumRows())
	assert.Equal(2, num)

	pr.ReadStop()
	assert.NoError(localFileReader.Close())

	operationsFile := ""
	for _, object := range objects {
		if strings.HasPrefix(object.key, "/operations") {
			operationsFile = object.fileName
		}
	}

	assert.NotEmpty(operationsFile)

	localFileReaderOperations, err := local.NewLocalFileReader(operationsFile)
	assert.NoError(err)
	prOperations, err := reader.NewParquetReader(localFileReaderOperations, new(OperationRecord), 1)
	assert.NoError(err)

	numOperations := int(prOperations.GetNumRows())
	assert.Equal(1, numOperations)

	prOperations.ReadStop()
	assert.NoError(localFileReaderOperations.Close())
}

func TestWriteSpanWithTagsAndReferences(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSvc := mocks.NewMockS3API(ctrl)

	assert := assert.New(t)
	ctx := context.TODO()

	file, err := ioutil.TempFile("", "write-span")
	assert.NoError(err)
	defer os.Remove(file.Name())

	mockSvc.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			dat, err := ioutil.ReadAll(input.Body)
			assert.NoError(err)
			assert.NoError(ioutil.WriteFile(file.Name(), dat, 0644))

			return &s3.PutObjectOutput{}, nil
		}).Times(2)

	writer := NewTestWriter(ctx, assert, mockSvc)

	span := NewTestSpanWithTagsAndReferences(assert)

	assert.NoError(writer.WriteSpan(ctx, span))
	assert.NoError(writer.Close())

	localFileReader, err := local.NewLocalFileReader(file.Name())
	assert.NoError(err)
	pr, err := reader.NewParquetReader(localFileReader, new(SpanRecord), 1)
	assert.NoError(err)

	num := int(pr.GetNumRows())
	assert.Equal(1, num)

	records := make([]SpanRecord, 1)
	assert.NoError(pr.Read(&records))

	record := records[0]

	assert.Equal("0000000000000012", record.TraceID)
	assert.Equal("0000000000000004", record.SpanID)
	assert.Equal("query12-operation", record.OperationName)
	assert.Equal("", record.SpanKind)
	assert.Equal(int64(1485449191639), record.StartTime)
	assert.Equal(int64(2000), record.Duration)
	assert.Equal(map[string]string{
		"blob": "00003039", "sameplacetag1": "sameplacevalue", "sameplacetag2": "123", "sameplacetag3": "72.5", "sameplacetag4": "true",
	}, record.Tags)
	assert.Equal("query12-service", record.ServiceName)
	assert.Equal([]SpanRecordReferences{
		{TraceID: "00000000000000ff", SpanID: "00000000000000ff", RefType: 0},
	}, record.References)

	pr.ReadStop()
	assert.NoError(localFileReader.Close())
}

func BenchmarkWriteSpan(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	assert := assert.New(b)
	ctx := context.TODO()

	mockSvc := mocks.NewMockS3API(ctrl)
	mockSvc.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&s3.PutObjectOutput{}, nil)

	writer := NewTestWriter(ctx, assert, mockSvc)
	span := NewTestSpan(assert)

	// run the WriteSpan function b.N times
	for n := 0; n < b.N; n++ {
		assert.NoError(writer.WriteSpan(ctx, span))
	}
	assert.NoError(writer.Close())
}

func BenchmarkWriteSpanParallel(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	assert := assert.New(b)
	ctx := context.TODO()

	mockSvc := mocks.NewMockS3API(ctrl)
	mockSvc.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&s3.PutObjectOutput{}, nil)

	writer := NewTestWriter(ctx, assert, mockSvc)
	defer writer.Close()
	span := NewTestSpan(assert)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			assert.NoError(writer.WriteSpan(ctx, span))
		}
	})
}
