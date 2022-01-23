package config

type Kinesis struct {
	SpanStreamName string
}

type Athena struct {
	DatabaseName   string
	TableName      string
	OutputLocation string
}

type Configuration struct {
	Kinesis Kinesis
	Athena  Athena
}
