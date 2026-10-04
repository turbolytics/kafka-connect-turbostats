# kafka-connect-turbostats

Reports every Kafka Connect connector task to TurboStats: whether it is running, failed or restarting, how much it read and wrote, and the worker's memory. One jar, no dependencies.

## Requirements

- **Java 17 or later on the worker.** Debezium 3 already requires it. On Java 11 the worker fails to start, with `UnsupportedClassVersionError` naming this jar.
- **Kafka Connect 2.3 or later**, distributed or standalone.

## Install it on the worker's classpath

**Put the jar in the worker's classpath, not its plugin path.** In the Debezium image that is `/kafka/libs`. Connect builds each task's producer and consumer with the connector's own classloader, which cannot see another plugin: from the plugin path, every source task on the worker fails with `Failed to construct kafka producer`.

Then add to the worker's properties:

```properties
rest.extension.classes=io.turbolytics.turbostats.connect.TurboStatsExtension
producer.interceptor.classes=io.turbolytics.turbostats.connect.intercept.AckInterceptor
consumer.interceptor.classes=io.turbolytics.turbostats.connect.intercept.ConsumeInterceptor

turbostats.report.to=https://ingest.turbolytics.io/v1/turbostats
turbostats.key=${env:TURBOSTATS_KEY}
config.providers=env
config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
```

If the worker already sets interceptor classes, add these to its list, comma-separated.

## Settings

| Property | Default | Means |
|---|---|---|
| `turbostats.report.to` | none: reporting is off | Where reports go. `https`, or `http` to the loopback only. |
| `turbostats.key` | none | The `sfc_` credential. Supply it through a config provider, never in plain text. |
| `turbostats.cluster` | the worker's `group.id` | The first part of every instance id. Set it when `group.id` is a stock value such as `connect-cluster`. |
| `turbostats.interval.seconds` | 60 | How often each task reports. |
| `turbostats.timeout.seconds` | 10 | How long one report may take. Less than the interval. |
| `turbostats.label.<key>` | none | Your own labels: at most 10, keys `[a-z][a-z0-9_]*`. |

A bad setting turns reporting off and logs why. It never stops the worker or a connector.

At startup the worker logs `reporting every 60s to … as Credential(<key id>)`. If that key id does not match the one your dashboard shows for the key, the JVM's crypto provider derived a different key from the credential; FIPS-mode and non-OpenJDK providers can.

## What a report carries

One report per task, under the id `<cluster>/<connector>/<task>`:

- The task's state: `running`, `paused`, `failed`, `starting` or `stopped`.
- Restarts: how many times the task started on this worker since it arrived here, after the first, and when it last started. A task that moves away and comes back starts counting afresh.
- Records read and written. A source task's written count is what the broker acknowledged. A sink task's is records read less records in flight, which also counts records a transform dropped, so it is a ceiling on rows delivered.
- Records discarded under `errors.tolerance=all`, and records sent to a dead letter queue.
- The worker's uptime, memory, garbage collections and container limit.
- The connector's type and a hash of its config, keyed by your credential. The config itself never leaves the worker: it holds your database password. The keyed hash means nothing outside your install, and cannot be checked against a guessed password.

When a connector is deleted or stopped, or `tasks.max` is lowered, each task that ends sends a final report saying so. A task that moves to another worker continues under the same id.

## Testing

Three layers, each its own command and CI job. Maven runs in Docker through `scripts/mvn`, so no local JDK is needed.

| Layer | Command | What it proves |
|---|---|---|
| Unit | `scripts/mvn test` | The reporter's logic, with no container. |
| Integration | `scripts/mvn verify -Pintegration` | The interceptors inside real Kafka clients against a real broker, and the metric reader against Kafka's own JMX reporter. |
| Release | `scripts/mvn verify -Prelease` | The built jar installed in the Debezium image, reporting to a fake control plane. Every post it received is in `target/release/posts.jsonl`. |
