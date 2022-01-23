# jaeger-s3

jaeger-s3 is gRPC storage plugin for [Jaeger](https://github.com/jaegertracing/jaeger), which uses [Amazon Kinesis Data Firehose](https://aws.amazon.com/kinesis/data-firehose/)
to store spans on [Amazon S3](https://aws.amazon.com/s3/). The stored spans are queried using [Amazon Athena](https://aws.amazon.com/athena/).

## Usage

### Prepare your environment

Prepare our environment using terraform a comparable tool. The setup instructions are not final yet and might change in the future.

TBD

### Install the plugin

```yaml
kind: ConfigMap
apiVersion: v1
metadata:
  name: jaeger-s3
  namespace: jaeger-collector
data:
  config.yaml: >
    kinesis:
      spanStreamName: jaeger
    athena:
      databaseName: default
      tableName: jaeger
      outputLocation: s3://athena-results-bucket/

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
      image: ghcr.io/johanneswuerbach/jaeger-s3:v0.0.1
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
