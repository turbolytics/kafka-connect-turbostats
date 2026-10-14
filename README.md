# kafka-connect-turbostats

Reports every Kafka Connect connector task to TurboStats: whether it is running, failed or restarting, how much it read and wrote, and the worker's memory. One jar, no dependencies.

## Requirements

Check the worker before you install:

- **Java 17 or later on the worker.** Debezium 3 already requires it. On Java 11 the worker fails to start, with `UnsupportedClassVersionError` naming this jar.
- **Kafka Connect 2.5 or later**, distributed or standalone. Sink lag asks the broker through `Admin.listOffsets` and `OffsetSpec`, which arrived in 2.5.

## Install

Install takes six steps, from no key to the first report:

1. **Create a key.** In TurboStats Control at https://control.turbolytics.io, open your org's Keys page and create a key. The page shows the `sfc_` credential once. Copy it then.
2. **Download the jar** from https://github.com/turbolytics/kafka-connect-turbostats/releases/latest.
3. **Put the jar on the worker's classpath, not its plugin path.** In the Debezium image that is `/kafka/libs`; [Platforms](#platforms) names the others. Connect builds each task's producer and consumer with the connector's own classloader, which cannot see another plugin: from the plugin path, every source task on the worker fails with `Failed to construct kafka producer`.
4. **Add to the worker's properties:**

   ```properties
   rest.extension.classes=io.turbolytics.turbostats.connect.TurboStatsExtension
   producer.interceptor.classes=io.turbolytics.turbostats.connect.intercept.AckInterceptor
   consumer.interceptor.classes=io.turbolytics.turbostats.connect.intercept.ConsumeInterceptor

   turbostats.report.to=https://ingest.turbolytics.io/v1/turbostats
   turbostats.key=${env:TURBOSTATS_KEY}
   config.providers=env
   config.providers.env.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider
   ```

   If the worker already sets `rest.extension.classes` or interceptor classes, add these to its list, comma-separated.
5. **Set `TURBOSTATS_KEY`** in the worker's environment to the credential from step 1.
6. **Restart the worker** and look for this line in its log:

   ```
   turbostats: reporting every 60s to https://ingest.turbolytics.io/v1/turbostats as Credential(<key id>) for cluster <cluster>
   ```

   The key id matches the one Control shows for the key. If the line says reporting is off, the `turbostats:` lines above it say why.

   The worker checks the key pair before it reports. A FIPS-mode or non-OpenJDK crypto provider can derive a different public key from the credential. The worker then logs `turbostats.key: the JVM's crypto provider <provider> derived a different key from the credential`, and reporting stays off.

## Platforms

**Debezium image.** Put the jar in `/kafka/libs`. Set worker properties as `CONNECT_*` variables, such as `CONNECT_TURBOSTATS_REPORT_TO`. Removing a `CONNECT_TURBOSTATS_*` variable does not remove the setting. The image appends each variable to the worker's properties file in the `/kafka/config` volume, and the volume survives a container recreate. Recreate with `docker compose up --force-recreate --renew-anon-volumes`, or edit the file.

**Confluent `cp-kafka-connect`.** Put the jar in `/usr/share/java/kafka`, which the image puts on the classpath. Set worker properties as `CONNECT_*` variables: `CONNECT_REST_EXTENSION_CLASSES`, `CONNECT_PRODUCER_INTERCEPTOR_CLASSES`, `CONNECT_TURBOSTATS_KEY` and so on. Confluent RBAC sets its own REST extension, and Confluent Monitoring Interceptors set interceptor classes. Comma-join ours with theirs.

**Strimzi.** Build a custom image with the jar in `/opt/kafka/libs`. `spec.build` puts jars on the plugin path, which does not work. Put the worker properties in `spec.config`, and `TURBOSTATS_KEY` in `spec.template.connectContainer.env` from a Secret. In the `KafkaConnect` spec, Strimzi allows `rest.extension.classes` but forbids `producer.interceptor.classes` and `consumer.interceptor.classes`. Without the interceptors, the reporter still reports each task's state, records and lag from Connect's metrics. It loses restarts, source event lag and snapshot progress. To keep them, set the interceptors per connector in each `KafkaConnector`:

- `producer.override.interceptor.classes=io.turbolytics.turbostats.connect.intercept.AckInterceptor` on a source.
- `consumer.override.interceptor.classes=io.turbolytics.turbostats.connect.intercept.ConsumeInterceptor` on a sink.

Overrides need `connector.client.config.override.policy=All` on the worker, the default since Kafka 3.0.

**MSK Connect.** Not supported. It offers no worker classpath and restricts worker properties.

## Settings

| Property | Default | Means |
|---|---|---|
| `turbostats.report.to` | none: reporting is off | Where reports go. `https`, or `http` to the loopback only. |
| `turbostats.key` | none | The `sfc_` credential. Supply it through a config provider, never in plain text. |
| `turbostats.cluster` | the worker's `group.id` | The first part of every instance id. Set it when `group.id` is a stock value such as `connect-cluster`. |
| `turbostats.interval.seconds` | 60 | How often each task reports. TurboStats Control accepts at most two reports a minute per task, so keep it at 30 or more. |
| `turbostats.timeout.seconds` | 10 | How long one report may take. Less than the interval. |
| `turbostats.label.<key>` | none | Your own labels: at most 10, keys `[a-z][a-z0-9_]*`. |

A bad setting turns reporting off and logs why. It never stops the worker or a connector.

## What a report carries

One report per task, under the id `<cluster>/<connector>/<task>`:

- The task's state: `running`, `paused`, `failed`, `starting` or `stopped`.
- Restarts: how many times the task started on this worker since it arrived here, after the first, and when it last started. A task that moves away and comes back starts counting afresh.
- Records read and written. A source task's written count is what the broker acknowledged. A sink task's is records read less records in flight, which also counts records a transform dropped, so it is a ceiling on rows delivered.
- Records discarded under `errors.tolerance=all`, and records sent to a dead letter queue.
- Event lag: how far behind the stream the task runs, in seconds, and what that is measured from. A Debezium source measures from the database's commit time. A sink measures from the record's Kafka timestamp.
- For a Debezium source, the snapshot's progress and whether the database connection is up. A task restarted mid-snapshot reports the snapshot as `unknown`, because Debezium does not register the restarted task's snapshot metrics.
- For a sink, its lag in messages, asked of the broker for the partitions the task holds. The reporter connects with the worker's own `bootstrap.servers`, `security.protocol`, `sasl.*` and `ssl.*` settings, and `admin.*` overrides, and needs permission to describe the sink's consumer group.
- Bytes the task's Kafka client sent or fetched.
- The worker's uptime, memory, garbage collections and container limit.
- The worker's `host:port`, as Connect names the worker.
- The connector's type and a hash of its config, keyed by your credential. The config itself never leaves the worker: it holds your database password. The keyed hash means nothing outside your install, and cannot be checked against a guessed password.

When a connector is deleted or stopped, or `tasks.max` is lowered, each task that ends sends a final report saying so. A task that moves to another worker continues under the same id.

Kafka logs each `turbostats.*` setting as unused, because Connect hands the worker's properties to its own Kafka clients. The line names the setting, never its value:

- Before Kafka 3.3: `The configuration 'turbostats.key' was supplied but isn't a known config.`, at WARN, once per setting.
- Kafka 3.3 and 3.4: `These configurations '[turbostats.key, …]' were supplied but are not used yet.`, at WARN.
- Kafka 3.5 and later: the same line, at INFO.

## Testing

Three layers, each its own command and CI job. Maven runs in Docker through `scripts/mvn`, so no local JDK is needed.

| Layer | Command | What it proves |
|---|---|---|
| Unit | `scripts/mvn test` | The reporter's logic, with no container. |
| Integration | `scripts/mvn verify -Pintegration` | The interceptors inside real Kafka clients against a real broker, and the metric reader against Kafka's own JMX reporter. |
| Release | `scripts/mvn verify -Prelease` | The built jar installed in the Debezium image, reporting to a fake control plane. Every post it received is in `target/release/posts.jsonl`. |
