# jaeger-s3

jaeger-s3 is gRPC storage plugin for [Jaeger](https://github.com/jaegertracing/jaeger), which uses [Amazon S3](https://aws.amazon.com/s3/)
to store spans. The stored spans are queried using [Amazon Athena](https://aws.amazon.com/athena/).


## Setup

* [Architecture](docs/architecture.md)
* [Performance](docs/performance.md)
* [Setup](docs/setup.md)


## Development

Use `make lint` and `make test` to check the code style and to run unit tests. `make test-jaeger-grpc-integration` allows to run end-to-end tests using real AWS services, but currently uses hardcoded bucket names.
