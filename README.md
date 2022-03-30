# jaeger-s3

jaeger-s3 is gRPC storage plugin for [Jaeger](https://github.com/jaegertracing/jaeger), which uses [Amazon S3](https://aws.amazon.com/s3/)
to store spans. [Amazon Athena](https://aws.amazon.com/athena/) is used to query the stored spans by the plugin and can be also used for
further analytical queries. The primary goal of this plugin is to provide a mostly maintenance free, pay-as-you-go solution for AWS where you
don't need to think about server sizing or software updates.


## Setup

* [Architecture](docs/architecture.md)
* [Performance](docs/performance.md)
* [Setup](docs/setup.md)


## Development

Use `make lint` and `make test` to check the code style and to run unit tests. `make test-jaeger-grpc-integration` allows to run end-to-end tests using real AWS services, but currently uses hardcoded bucket names.
