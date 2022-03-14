# Setup

## Prepare your environment

Prepare our environment using terraform a comparable tool. The setup instructions are not final yet and might change in the future.

### Required resources

Create an S3 bucket, a Glue table and an Athena Workgroup.

```tf
locals {
  bucket_name                = "my-jaeger-s3-bucket"
  bucket_name_athena_results = "my-jaeger-s3-bucket-athena-results"
}

resource "aws_s3_bucket" "jaeger" {
  bucket = local.bucket_name

  versioning {
    enabled = true
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }

  lifecycle_rule {
    id      = "retention"
    enabled = true

    expiration {
      days = 14
    }

    abort_incomplete_multipart_upload_days = 1

    noncurrent_version_expiration {
      days = 1
    }
  }

  lifecycle_rule {
    id      = "delete-deleted"
    enabled = true

    expiration {
      expired_object_delete_marker = true
    }
  }

  tags = {
    managed_by = "terraform"
  }
}


resource "aws_s3_bucket" "jaeger_athena_results" {
  bucket = local.bucket_name_athena_results

  versioning {
    enabled = true
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }

  tags = {
    managed_by = "terraform"
  }
}

resource "aws_glue_catalog_table" "jaeger_spans" {
  name          = "jaeger_spans"
  database_name = "default"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"                    = "parquet",
    "projection.enabled"                = "true",
    "projection.datehour.type"          = "date",
    "projection.datehour.format"        = "yyyy/MM/dd/HH",
    "projection.datehour.range"         = "2022/01/01/00,NOW",
    "projection.datehour.interval"      = "1",
    "projection.datehour.interval.unit" = "HOURS",
    "storage.location.template"         = "s3://${aws_s3_bucket.jaeger.id}/spans/$${datehour}/"
  }

  partition_keys {
    name = "datehour"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.jaeger.id}/spans/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1,
      }
    }

    columns {
      name = "trace_id"
      type = "string"
    }
    columns {
      name = "span_id"
      type = "string"
    }
    columns {
      name = "operation_name"
      type = "string"
    }
    columns {
      name = "span_kind"
      type = "string"
    }
    columns {
      name = "start_time"
      type = "timestamp"
    }
    columns {
      name = "duration"
      type = "bigint"
    }
    columns {
      name = "tags"
      type = "map<string,string>"
    }
    columns {
      name = "service_name"
      type = "string"
    }
    columns {
      name = "span_payload"
      type = "string"
    }
    columns {
      name = "references"
      type = "array<struct<trace_id:string,span_id:string,ref_type:tinyint>>"
    }
  }
}

resource "aws_glue_catalog_table" "jaeger_operations" {
  name          = "jaeger_operations"
  database_name = "default"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"                    = "parquet",
    "projection.enabled"                = "true",
    "projection.datehour.type"          = "date",
    "projection.datehour.format"        = "yyyy/MM/dd/HH",
    "projection.datehour.range"         = "2022/01/01/00,NOW",
    "projection.datehour.interval"      = "1",
    "projection.datehour.interval.unit" = "HOURS",
    "storage.location.template"         = "s3://${aws_s3_bucket.jaeger.id}/operations/$${datehour}/"
  }

  partition_keys {
    name = "datehour"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.jaeger.id}/operations/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = 1,
      }
    }

    columns {
      name = "operation_name"
      type = "string"
    }
    columns {
      name = "span_kind"
      type = "string"
    }
    columns {
      name = "service_name"
      type = "string"
    }
  }
}

resource "aws_athena_workgroup" "jaeger" {
  name = "jaeger"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.jaeger_athena_results.bucket}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
```

### Role for jaeger pods

Create a role to be used by your jaeger collector and query pods.

```tf
locals {
  bucket_prefix = "my-jaeger-s3-bucket"
}

data "aws_iam_policy_document" "jaeger" {

  # Span writer

  statement {
    actions = [
      "s3:PutObject",
      "s3:PutObjectAcl",
      "s3:GetObject",
      "s3:GetObjectAcl",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",

      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutBucketPublicAccessBlock",
    ]

    resources = [
      "arn:aws:s3:::${local.bucket_prefix}-*"
    ]
  }

  # Span reader

  statement {
    actions = [
      "athena:GetWorkGroup",
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:ListQueryExecutions",
    ]

    resources = [
      "arn:aws:athena:*:*:workgroup/jaeger"
    ]
  }

  statement {
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
    ]

    resources = [
      "arn:aws:glue:*:*:catalog",
      "arn:aws:glue:*:*:database/default",
      "arn:aws:glue:*:*:table/default/jaeger*"
    ]
  }
}

data "aws_caller_identity" "current" {
}

data "aws_iam_policy_document" "k8s_nodes_assumerole" {
  statement {
    actions = [
      "sts:AssumeRole",
    ]

    principals {
      type        = "AWS"
      identifiers = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "jaeger" {
  name               = "jaeger"
  assume_role_policy = data.aws_iam_policy_document.k8s_nodes_assumerole.json
}

resource "aws_iam_role_policy" "jaeger" {
  name   = "jaeger"
  role   = aws_iam_role.jaeger.id
  policy = data.aws_iam_policy_document.jaeger.json
}
```

## Install the plugin

Install the plugin in your jaeger installation.

```yaml
kind: ConfigMap
apiVersion: v1
metadata:
  name: jaeger-s3
  namespace: jaeger-collector
data:
  config.yaml: >
    s3:
      bucketName: my-jaeger-s3-bucket
      spansPrefix: spans/
      operationsPrefix: operations/
      bufferDuration: 60s
    athena:
      databaseName: default
      spansTableName: jaeger_spans
      operationsTableName: jaeger_operations
      outputLocation: s3://my-jaeger-s3-bucket-athena-results/
      workGroup: jaeger
      maxSpanAge: 336h
      dependenciesQueryTtl: 24h
      dependenciesPrefetch: true
      servicesQueryTtl: 60s

---
apiVersion: v1
kind: Secret
metadata:
  name: jaeger
  namespace: jaeger-collector
type: Opaque
data:
  AWS_REGION: ZXUtd2VzdC0x # encode your region (us-east-1) in this case
---
apiVersion: jaegertracing.io/v1
kind: Jaeger
metadata:
  name: jaeger
  namespace: jaeger-collector
spec:
  strategy: production
  collector:
    maxReplicas: 10
    options:
      collector:
        # queue size and memory requests / limits based on
        # https://github.com/jaegertracing/jaeger-operator/issues/872#issuecomment-596618094
        queue-size-memory: 64
    resources:
      requests:
        memory: 128Mi
        cpu: "150m"
      limits:
        memory: 512Mi
        cpu: "500m"
  query:
    replicas: 2
    resources:
      requests:
        memory: 125Mi
        cpu: "150m"
      limits:
        memory: 1024Mi
        cpu: "500m"
  annotations:
    iam.amazonaws.com/role: jaeger
  storage:
    type: grpc-plugin
    grpcPlugin:
      image: ghcr.io/johanneswuerbach/jaeger-s3:v0.4.4
    options:
      grpc-storage-plugin:
        binary: /plugin/jaeger-s3
        configuration-file: /plugin-config/config.yaml
        log-level: debug
    esIndexCleaner:
      enabled: false
    dependencies:
      enabled: false
    # Not really a secret, but there is no other way to get environment
    # variables into the container currently
    secretName: jaeger
  volumeMounts:
    - name: plugin-config
      mountPath: /plugin-config
  volumes:
    - name: plugin-config
      configMap:
        name: jaeger-s3
```
