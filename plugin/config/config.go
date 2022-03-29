package config

type S3 struct {
	BucketName                string
	SpansPrefix               string
	OperationsPrefix          string
	BufferDuration            string
	EmptyBucket               bool
	OperationsDedupeDuration  string
	OperationsDedupeCacheSize int
}

type Athena struct {
	DatabaseName         string
	SpansTableName       string
	OperationsTableName  string
	WorkGroup            string
	OutputLocation       string
	MaxSpanAge           string
	DependenciesQueryTTL string
	ServicesQueryTTL     string
	MaxTraceDuration     string
	DependenciesPrefetch bool
}

type Configuration struct {
	S3     S3
	Athena Athena
}
