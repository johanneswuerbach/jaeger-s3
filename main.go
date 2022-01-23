package main

import (
	"context"
	"log"
	"os"

	"github.com/johanneswuerbach/jaeger-s3/plugin"
	pConfig "github.com/johanneswuerbach/jaeger-s3/plugin/config"
	"github.com/ory/viper"
	"github.com/spf13/pflag"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
)

const (
	loggerName = "jaeger-s3"
)

func main() {
	logLevel := os.Getenv("GRPC_STORAGE_PLUGIN_LOG_LEVEL")
	if logLevel == "" {
		logLevel = hclog.Warn.String()
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.LevelFromString(logLevel),
		Name:       loggerName,
		JSONFormat: true,
	})

	var configPath string
	pflag.StringVar(&configPath, "config", "", "A path to the s3 plugin's configuration file")
	pflag.Parse()
	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		log.Fatalf("unable bind flags, %v", err)
	}

	if configPath != "" {
		viper.SetConfigFile(configPath)

		if err := viper.ReadInConfig(); err != nil {
			log.Fatalf("error reading config file, %v", err)
		}
	}

	var configuration pConfig.Configuration
	err := viper.Unmarshal(&configuration)
	if err != nil {
		log.Fatalf("unable to decode into struct, %v", err)
	}

	logger.Debug("plugin starting ...", configuration)

	ctx := context.TODO()

	cfg, err := config.LoadDefaultConfig(ctx, func(lo *config.LoadOptions) error {
		return nil
	})
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	firehoseSvc := firehose.NewFromConfig(cfg)
	athenaSvc := athena.NewFromConfig(cfg)

	logger.Debug("plugin configured")

	s3Plugin, err := plugin.NewS3Plugin(logger, firehoseSvc, configuration.Kinesis, athenaSvc, configuration.Athena)
	if err != nil {
		log.Fatalf("unable to create plugin, %v", err)
	}

	logger.Debug("plugin created")
	grpc.Serve(&shared.PluginServices{
		Store: s3Plugin,
	})
}
