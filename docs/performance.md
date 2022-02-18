# Performance

## Production ready?

The plugin is being successfully used in an environment with ~ 5000 spans/s
and 14 days of retention. Nevertheless you should be doing your own tests
to validate whether it fits your environment.

## Improve query performance

You can do multiple things to improve query performance, but they all of same target of reducing the amount of files created on S3.

### Fewer, larger collectors

As every collector creates a separate parquet file per rotation, reducing the numbers of collectors also reduced the amount of files.

You can do this in the jaeger operator like:

```yaml
kind: Jaeger
metadata:
  name: jaeger
  ...
spec:
  strategy: production
  collector:
    ...
    options:
      collector:
        # queue size and memory requests / limits based on
        # https://github.com/jaegertracing/jaeger-operator/issues/872#issuecomment-596618094
        queue-size-memory: 256
    resources:
      requests:
        memory: 512Mi # queue size * 2
        cpu:  600m
      limits:
        memory: 2048Mi # request * 4
        cpu: 2000m
  query:
  ...
```
