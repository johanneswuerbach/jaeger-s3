package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenaTypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	glueTypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx, func(lo *config.LoadOptions) error {
		return nil
	})
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	s3Svc := s3.NewFromConfig(cfg)
	athenaSvc := athena.NewFromConfig(cfg)
	glueSvc := glue.NewFromConfig(cfg)

	bucketName := "jaeger-s3-test"
	bucketNameResults := "jaeger-s3-test-results"

	_, err = s3Svc.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		log.Fatalf("unable to create s3 bucket, %v", err)
	}

	_, err = s3Svc.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketNameResults),
	})
	if err != nil {
		log.Fatalf("unable to create s3 bucket, %v", err)
	}

	_, err = glueSvc.CreateDatabase(ctx, &glue.CreateDatabaseInput{
		DatabaseInput: &glueTypes.DatabaseInput{
			Name: aws.String("default"),
		},
	})
	if err != nil {
		var bne *glueTypes.AlreadyExistsException
		if !errors.As(err, &bne) {
			log.Fatalf("unable to create glue database, %v", err)
		}
	}

	_, err = glueSvc.DeleteTable(ctx, &glue.DeleteTableInput{
		DatabaseName: aws.String("default"),

		Name: aws.String("jaeger_spans"),
	})
	if err != nil {
		var bne *glueTypes.EntityNotFoundException
		if !errors.As(err, &bne) {
			log.Fatalf("unable to delete glue table, %v", err)
		}
	}

	_, err = glueSvc.CreateTable(ctx, &glue.CreateTableInput{
		DatabaseName: aws.String("default"),

		TableInput: &glueTypes.TableInput{
			Name: aws.String("jaeger_spans"),

			Parameters: map[string]string{
				"classification":                    "parquet",
				"projection.enabled":                "true",
				"projection.datehour.type":          "date",
				"projection.datehour.format":        "yyyy/MM/dd/HH",
				"projection.datehour.range":         "2022/01/01/00,NOW",
				"projection.datehour.interval":      "1",
				"projection.datehour.interval.unit": "HOURS",
				"storage.location.template":         fmt.Sprintf("s3://%s/spans/${datehour}/", bucketName),
			},

			PartitionKeys: []glueTypes.Column{
				{
					Name: aws.String("datehour"),
					Type: aws.String("string"),
				},
			},

			StorageDescriptor: &glueTypes.StorageDescriptor{
				Location:     aws.String(fmt.Sprintf("s3://%s/spans/", bucketName)),
				InputFormat:  aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
				OutputFormat: aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),

				SerdeInfo: &glueTypes.SerDeInfo{
					SerializationLibrary: aws.String("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
					Parameters: map[string]string{
						"serialization.format": "1",
					},
				},

				Columns: []glueTypes.Column{
					{
						Name: aws.String("trace_id"),
						Type: aws.String("string"),
					},
					{
						Name: aws.String("span_id"),
						Type: aws.String("string"),
					},
					{
						Name: aws.String("operation_name"),
						Type: aws.String("string"),
					},
					{
						Name: aws.String("span_kind"),
						Type: aws.String("string"),
					},
					{
						Name: aws.String("start_time"),
						Type: aws.String("timestamp"),
					},
					{
						Name: aws.String("duration"),
						Type: aws.String("bigint"),
					},
					{
						Name: aws.String("tags"),
						Type: aws.String("map<string,string>"),
					},
					{
						Name: aws.String("service_name"),
						Type: aws.String("string"),
					},
					{
						Name: aws.String("span_payload"),
						Type: aws.String("string"),
					},
					{
						Name: aws.String("references"),
						Type: aws.String("array<struct<trace_id:string,span_id:string,ref_type:tinyint>>"),
					},
				},
			},
		},
	})
	if err != nil {
		log.Fatalf("unable to create glue table, %v", err)
	}

	_, err = glueSvc.DeleteTable(ctx, &glue.DeleteTableInput{
		DatabaseName: aws.String("default"),

		Name: aws.String("jaeger_operations"),
	})
	if err != nil {
		var bne *glueTypes.EntityNotFoundException
		if !errors.As(err, &bne) {
			log.Fatalf("unable to delete glue table, %v", err)
		}
	}

	_, err = glueSvc.CreateTable(ctx, &glue.CreateTableInput{
		DatabaseName: aws.String("default"),

		TableInput: &glueTypes.TableInput{
			Name: aws.String("jaeger_operations"),

			Parameters: map[string]string{
				"classification":                    "parquet",
				"projection.enabled":                "true",
				"projection.datehour.type":          "date",
				"projection.datehour.format":        "yyyy/MM/dd/HH",
				"projection.datehour.range":         "2022/01/01/00,NOW",
				"projection.datehour.interval":      "1",
				"projection.datehour.interval.unit": "HOURS",
				"storage.location.template":         fmt.Sprintf("s3://%s/operations/${datehour}/", bucketName),
			},

			PartitionKeys: []glueTypes.Column{
				{
					Name: aws.String("datehour"),
					Type: aws.String("string"),
				},
			},

			StorageDescriptor: &glueTypes.StorageDescriptor{
				Location:     aws.String(fmt.Sprintf("s3://%s/operations/", bucketName)),
				InputFormat:  aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
				OutputFormat: aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),

				SerdeInfo: &glueTypes.SerDeInfo{
					SerializationLibrary: aws.String("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
					Parameters: map[string]string{
						"serialization.format": "1",
					},
				},

				Columns: []glueTypes.Column{
					{
						Name: aws.String("operation_name"),
						Type: aws.String("string"),
					},
					{
						Name: aws.String("span_kind"),
						Type: aws.String("string"),
					},
					{
						Name: aws.String("service_name"),
						Type: aws.String("string"),
					},
					{
						Name: aws.String("span_payload"),
						Type: aws.String("string"),
					},
				},
			},
		},
	})
	if err != nil {
		log.Fatalf("unable to create glue table, %v", err)
	}

	_, err = athenaSvc.CreateWorkGroup(ctx, &athena.CreateWorkGroupInput{
		Name: aws.String("jaeger"),
		Configuration: &athenaTypes.WorkGroupConfiguration{
			ResultConfiguration: &athenaTypes.ResultConfiguration{
				OutputLocation: aws.String(fmt.Sprintf("s3://%s/", bucketNameResults)),
			},
		},
	})
	if err != nil {
		var bne *athenaTypes.InvalidRequestException
		if !errors.As(err, &bne) {
			log.Fatalf("unable to create jaeger work group, %v", err)
		}
	}
}
