package config

type S3 struct {
	BucketName     string
	Prefix         string
	BufferDuration string
	EmptyBucket    bool
}

type Athena struct {
	DatabaseName         string
	TableName            string
	WorkGroup            string
	OutputLocation       string
	MaxSpanAge           string
	DependenciesQueryTTL string
	ServicesQueryTTL     string
}

type Configuration struct {
	S3     S3
	Athena Athena
}
