# Performance

## Production ready?

The plugin is being successfully used in an environment with ~5000 spans/s and 14 days of retention
(~7 TB data on S3). Nevertheless you should be doing your own tests to validate whether it fits your environment.

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

### Avoid high cardinality operation names

As the Jaeger UI needs to load a service and operation dropdown before the user can make any input, ensuring performance of this operation
is key to the user experience.

Unique service name, operation name and span kind writes are already deduplicated by default, but using high cardinality operation names on
S3 will result in a lot of files on S3, which decreases query performance (See Tip 4. [here](https://aws.amazon.com/blogs/big-data/top-10-performance-tuning-tips-for-amazon-athena/)).

You can use the following query to get the amount of unique operation name and span kind combinations by service:

```sql
WITH uniq_pairs AS (
    SELECT DISTINCT service_name, operation_name, span_kind FROM "jaeger_operations"
)
SELECT service_name, COUNT(*) AS paris FROM uniq_pairs GROUP BY 1 ORDER BY 2 DESC
```
