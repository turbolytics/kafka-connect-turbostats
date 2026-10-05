# Kafka Connect reporter, Plan B: lag and snapshot progress Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every Debezium source task reports its event lag, snapshot progress and database connection; every sink task reports its lag in messages from the broker and its lag in time from its consumer; every task reports its wire bytes.

**Architecture:** Three new readers feed the existing `TaskCollector`. `DebeziumMetrics` reads Debezium's MBeans for a task and marks stale ones by registration time. `BrokerLag` asks the broker, once per tick, which partitions each sink task holds and how far behind they are. The consumer interceptor records each sink batch's newest record timestamp. `Pipeline` gains three nested records, `EventLag`, `MessageLag` and `Backfill`, which flatten into the contract's fields.

**Tech Stack:** Java 17, Kafka clients `Admin` (provided), JMX, JUnit 5, Testcontainers 2.0.5.

**Spec:** `turbolytics/sql-flow` `docs/superpowers/specs/2026-10-03-turbostats-kafka-connect-design.md`, sections "Kafka Connect and Debezium", "Backfill", "Event lag", and the spike findings in sql-flow PR #442. Memory: `kafka-connect-spike-findings`.

## Global Constraints

- Everything in Plan A's constraints holds: zero runtime dependencies, nothing throws into the worker, absent is never sent as zero, the jar stays on the worker classpath.
- A field the reporter cannot read this tick is absent, never a stale value: a Debezium MBean registered before the task's last start is stale.
- A Debezium source connector always sends `backfill`; without current snapshot metrics its state is `unknown`.
- Event lag basis: `source_commit_time` for Debezium, `kafka_create_time` or `kafka_log_append_time` for sinks, by the record's timestamp type.
- The admin client uses only the worker's connection settings: `bootstrap.servers`, `security.protocol`, `client.dns.lookup`, `sasl.*`, `ssl.*`, with `admin.*` overrides; every admin call is bounded by the report timeout.
- Tests: run only the tests a task touches; CI runs unit, integration and release. Say in one line what was not run.

## Review Focus

1. **A connector whose `topic.prefix` matches another connector's MBeans**, for example a renamed connector reusing a prefix. Only MBeans whose `server` equals this connector's prefix count. Pinned in Task 3: `DebeziumMetricsTest.onlyThisPrefixCounts`.
2. **A broker that times out.** Sink lag is absent for that tick; nothing else in the bundle is lost. Pinned in Task 5: `BrokerLagTest.aTimeoutLeavesLagAbsent`.
3. **A sink partition with no committed offset yet.** Its lag is unknown and it is left out, not counted as zero or as the whole topic. Pinned in Task 5: `BrokerLagTest.aPartitionWithoutACommitIsLeftOut`.
4. **A record with no timestamp (`NO_TIMESTAMP_TYPE`, -1).** No event lag from it. Pinned in Task 6: `ConsumeLagTest.recordsWithoutTimestampsGiveNoLag`.
5. **`MilliSecondsBehindSource` of -1** before the first streamed event. Event lag is absent, not -0.001 s. Pinned in Task 3: `DebeziumMapTest.noEventYetIsNoLag`.

---

### Task 1: The wire carries lag, event lag, backfill and wire bytes

**Files:**
- Modify: `src/main/java/io/turbolytics/turbostats/connect/json/JsonObject.java` (a `Double` overload)
- Create: `src/main/java/io/turbolytics/turbostats/connect/wire/EventLag.java`, `MessageLag.java`, `Backfill.java`
- Modify: `src/main/java/io/turbolytics/turbostats/connect/wire/Pipeline.java`, `src/main/java/io/turbolytics/turbostats/connect/collect/TaskCollector.java` (constructor call), `src/test/java/.../wire/Fixtures.java`
- Test: `src/test/java/.../wire/BundleSchemaTest.java`, `src/test/java/.../json/JsonObjectTest.java`

**Interfaces:**
- Produces:
  - `JsonObject put(String, Double)`, writing nothing for null, NaN or infinite.
  - `record EventLag(Double seconds, Double maxSeconds, Instant observedAt, String basis)`.
  - `record MessageLag(long maxMessages, long totalMessages, int partitions, Instant observedAt)`.
  - `record Backfill(String state, boolean blocksStream, Long elapsedSeconds, String unit, Integer unitsTotal, Integer unitsLeft, Long rowsRead)` with `static Backfill unknown()` and `static Backfill none()`.
  - `Pipeline` gains, after `dlqRows`: `EventLag eventLag, MessageLag messageLag, Backfill backfill, Boolean sourceConnected, Long sourceWireBytes, Long sinkWireBytes`. Its JSON flattens `eventLag` into `event_lag_seconds`, `event_lag_max_seconds`, `event_lag_observed_at`, `event_lag_basis`, and `messageLag` into `lag_max_messages`, `lag_total_messages`, `lag_partitions`, `lag_observed_at`. `backfill` is a nested object.

- [ ] **Step 1: Write the failing tests**

In `JsonObjectTest`:

```java
    // Event lag is fractional seconds. A non-finite double is not JSON.
    @Test
    void doublesAreWrittenAndNonFiniteIsAbsent() {
        Double nan = Double.NaN;
        Double none = null;
        assertEquals("{\"a\":1.5}", new JsonObject().put("a", 1.5).put("b", nan).put("c", none).toJson());
    }
```

In `Fixtures`, give both pipelines the new arguments. `sourceBundle()`'s `Pipeline` ends:

```java
                        55_000, 55_000, 0, T, T, null, 0L, 0L,
                        new EventLag(0.8, 41.2, T, "source_commit_time"),
                        null,
                        new Backfill("completed", false, 12L, "table", 3, 0, 55_000L),
                        true,
                        null,
                        412_000L),
```

`sinkBundle()`'s ends:

```java
                        126_624, 126_350, 0, T, T, null, 0L, 0L,
                        new EventLag(0.04, 3.1, T, "kafka_create_time"),
                        new MessageLag(274, 300, 4, T),
                        null,
                        null,
                        98_000L,
                        null),
```

In `BundleSchemaTest`:

```java
    // The flattened lag fields and the backfill object are the contract's
    // names, and validate.
    @Test
    void lagAndBackfillUseTheContractsNames() throws Exception {
        JsonNode src = MAPPER.readTree(Fixtures.sourceBundle().toJson());
        assertEquals("source_commit_time", src.at("/pipeline/event_lag_basis").asText());
        assertEquals(0.8, src.at("/pipeline/event_lag_seconds").asDouble());
        assertEquals("completed", src.at("/pipeline/backfill/state").asText());
        assertEquals(3, src.at("/pipeline/backfill/units_total").asInt());
        assertTrue(src.at("/pipeline/source_connected").asBoolean());
        assertEquals(412_000, src.at("/pipeline/sink_wire_bytes").asLong());
        JsonNode snk = MAPPER.readTree(Fixtures.sinkBundle().toJson());
        assertEquals(274, snk.at("/pipeline/lag_max_messages").asLong());
        assertEquals(4, snk.at("/pipeline/lag_partitions").asInt());
        assertFalse(snk.get("pipeline").has("backfill"));
        assertEquals(Set.of(), validate(Fixtures.sourceBundle().toJson()));
        assertEquals(Set.of(), validate(Fixtures.sinkBundle().toJson()));
    }

    // A backfill whose metrics are missing says unknown, and validates.
    @Test
    void anUnknownBackfillValidates() throws Exception {
        String json = new JsonObject().put("backfill", Backfill.unknown().toJson()).toJson();
        assertEquals("{\"backfill\":{\"state\":\"unknown\",\"blocks_stream\":false}}", json);
    }
```

Run: `scripts/mvn -q test -Dtest='JsonObjectTest,BundleSchemaTest'`
Expected: FAIL to compile with `cannot find symbol: class EventLag`.

- [ ] **Step 2: Implement**

`JsonObject`:

```java
    public JsonObject put(String key, Double value) {
        if (value != null && Double.isFinite(value)) {
            field(key).append(value.doubleValue());
        }
        return this;
    }
```

`EventLag.java`:

```java
package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;
import java.time.Instant;

/**
 * How far behind the stream a task runs, in time. The four fields travel
 * together: a reading without its basis cannot be compared, and one without
 * its time cannot be judged for age.
 */
public record EventLag(Double seconds, Double maxSeconds, Instant observedAt, String basis) {

    void writeInto(JsonObject o) {
        o.put("event_lag_seconds", seconds)
                .put("event_lag_max_seconds", maxSeconds)
                .put("event_lag_observed_at", observedAt)
                .put("event_lag_basis", basis);
    }
}
```

`MessageLag.java`:

```java
package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;
import java.time.Instant;

/**
 * A sink task's lag in messages, from the broker: the worst partition, the
 * sum, how many partitions they summarize, and when the broker answered.
 */
public record MessageLag(long maxMessages, long totalMessages, int partitions, Instant observedAt) {

    void writeInto(JsonObject o) {
        o.put("lag_max_messages", maxMessages)
                .put("lag_total_messages", totalMessages)
                .put("lag_partitions", (long) partitions)
                .put("lag_observed_at", observedAt);
    }
}
```

`Backfill.java`:

```java
package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;

/**
 * Bounded work inside the pipeline: a Debezium snapshot. state is none,
 * running, paused, completed, aborted, or unknown when the snapshot metrics
 * are missing or belong to an earlier run. blocksStream means nothing
 * unless the state is running or paused.
 */
public record Backfill(String state, boolean blocksStream, Long elapsedSeconds, String unit, Integer unitsTotal,
        Integer unitsLeft, Long rowsRead) {

    public static Backfill unknown() {
        return new Backfill("unknown", false, null, null, null, null, null);
    }

    public static Backfill none() {
        return new Backfill("none", false, null, null, null, null, null);
    }

    public JsonObject toJson() {
        return new JsonObject()
                .put("state", state)
                .put("blocks_stream", blocksStream)
                .put("elapsed_seconds", elapsedSeconds)
                .put("unit", unit)
                .put("units_total", unitsTotal == null ? null : (long) unitsTotal)
                .put("units_left", unitsLeft == null ? null : (long) unitsLeft)
                .put("rows_read", rowsRead);
    }
}
```

`Pipeline`: add the six components after `Long dlqRows`; pass them through `withState`; in `toJson`, build the object into a local `JsonObject o`, then:

```java
        if (eventLag != null) {
            eventLag.writeInto(o);
        }
        if (messageLag != null) {
            messageLag.writeInto(o);
        }
        return o.put("backfill", backfill == null ? null : backfill.toJson())
                .put("source_connected", sourceConnected)
                .put("source_wire_bytes", sourceWireBytes)
                .put("sink_wire_bytes", sinkWireBytes);
```

`JsonObject` gains `put(String, Boolean)` writing nothing for null, used by `source_connected`.

`TaskCollector`'s `new Pipeline(...)` passes `null, null, null, null, null, null` for now.

- [ ] **Step 3: Run the tests and commit**

Run: `scripts/mvn -q test -Dtest='JsonObjectTest,BundleSchemaTest,TaskCollectorTest'`
Expected: PASS.

```bash
git add src && git commit -m "wire: the bundle carries lag, event lag, backfill and wire bytes"
```

---

### Task 2: When each Debezium MBean registered

**Files:**
- Modify: `src/main/java/.../collect/Jmx.java`, `PlatformJmx.java`, `src/test/java/.../collect/FakeJmx.java`
- Test: `src/test/java/.../collect/PlatformJmxTest.java`

**Interfaces:**
- Produces: `Jmx.registeredAt(ObjectName name): long`, the epoch milliseconds the MBean registered, or the time `PlatformJmx` started listening for one registered before. `FakeJmx.registeredAt(String objectName, long millis)` sets it; unset names return `Long.MAX_VALUE`, which is never stale.

- [ ] **Step 1: Write the failing test**

```java
package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.junit.jupiter.api.Test;

class PlatformJmxTest {
    public interface ProbeMBean {
        int getValue();
    }

    public static final class Probe implements ProbeMBean {
        public int getValue() {
            return 1;
        }
    }

    // A restarted task's old Debezium MBeans outlived its start by 80 s in
    // the spike. Registration time is how the reporter tells them apart.
    @Test
    void recordsWhenAnMBeanRegistered() throws Exception {
        PlatformJmx jmx = new PlatformJmx();
        long before = System.currentTimeMillis();
        ObjectName n = new ObjectName("debezium.test:type=connector-metrics,context=streaming,server=pj");
        ManagementFactory.getPlatformMBeanServer().registerMBean(new StandardMBean(new Probe(), ProbeMBean.class), n);
        try {
            long at = jmx.registeredAt(n);
            assertTrue(at >= before && at <= System.currentTimeMillis(), "registeredAt " + at);
        } finally {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(n);
        }
    }
}
```

Run: `scripts/mvn -q test -Dtest=PlatformJmxTest`
Expected: FAIL to compile with `cannot find symbol: method registeredAt`.

- [ ] **Step 2: Implement**

`Jmx` gains:

```java
    /**
     * When the MBean registered, in epoch milliseconds, or when listening
     * began for one registered before. A Debezium MBean registered before
     * its task's last start belongs to the run before it.
     */
    long registeredAt(ObjectName name);
```

`PlatformJmx`: a `ConcurrentHashMap<ObjectName, Long> registered`, a `long since = System.currentTimeMillis()` set in the constructor, and in the constructor:

```java
        try {
            server.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, (n, hb) -> {
                if (n instanceof MBeanServerNotification m && m.getMBeanName().getDomain().startsWith("debezium")) {
                    if (MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(m.getType())) {
                        registered.put(m.getMBeanName(), System.currentTimeMillis());
                    } else {
                        registered.remove(m.getMBeanName());
                    }
                }
            }, null, null);
        } catch (Exception ignored) {
            // Without notifications every MBean reads as registered at
            // start: none is ever judged stale, as before this existed.
        }
```

and

```java
    @Override
    public long registeredAt(ObjectName name) {
        return registered.getOrDefault(name, since);
    }
```

`FakeJmx`: a `Map<ObjectName, Long> registered`, `public FakeJmx registeredAt(String objectName, long millis)`, and `registeredAt(ObjectName)` returning `registered.getOrDefault(name, Long.MAX_VALUE)`.

- [ ] **Step 3: Run the test and commit**

Run: `scripts/mvn -q test -Dtest='PlatformJmxTest,ConnectMetricsTest'`
Expected: PASS.

```bash
git add src && git commit -m "collect: record when each Debezium MBean registered"
```

---

### Task 3: Read Debezium's metrics for a task

**Files:**
- Create: `src/main/java/.../collect/DebeziumMetrics.java`
- Test: `src/test/java/.../collect/DebeziumMetricsTest.java`, `DebeziumMapTest.java`

**Interfaces:**
- Consumes: `Jmx` (Task 2), `TaskKey`, `EventLag`, `Backfill` (Task 1).
- Produces: `DebeziumMetrics(Jmx)` with `View read(String topicPrefix, TaskKey k, long taskStartMillis)`; `record View(List<Map<String,Object>> snapshot, boolean snapshotStale, List<Map<String,Object>> streaming, boolean streamingStale)`; static mappers `Backfill backfill(View v)`, `EventLag eventLag(View v, Instant now)`, `Boolean sourceConnected(View v, Backfill b)`, `Instant lastEvent(View v, Instant now)`.

- [ ] **Step 1: Write the failing tests**

`DebeziumMetricsTest`:

```java
package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DebeziumMetricsTest {
    static final String SNAP = "debezium.postgres:type=connector-metrics,context=snapshot,server=inv";
    static final String STREAM = "debezium.postgres:type=connector-metrics,context=streaming,server=inv";
    static final TaskKey K = new TaskKey("inventory-cdc", 0);

    // Single-task connectors carry no task key: their MBeans are task 0's.
    @Test
    void readsBothContextsByPrefix() {
        FakeJmx jmx = new FakeJmx().put(SNAP, "SnapshotCompleted", true).put(STREAM, "Connected", true);
        DebeziumMetrics.View v = new DebeziumMetrics(jmx).read("inv", K, 0);
        assertEquals(1, v.snapshot().size());
        assertEquals(1, v.streaming().size());
        assertFalse(v.snapshotStale());
    }

    // Review focus: another connector's prefix is another connector's.
    @Test
    void onlyThisPrefixCounts() {
        FakeJmx jmx = new FakeJmx().put(SNAP, "SnapshotCompleted", true)
                .put("debezium.postgres:type=connector-metrics,context=streaming,server=inv2", "Connected", true);
        DebeziumMetrics.View v = new DebeziumMetrics(jmx).read("inv", K, 0);
        assertEquals(0, v.streaming().size());
    }

    // SQL Server and MongoDB add task=; only this task's count.
    @Test
    void aTaskKeySelectsTheTask() {
        FakeJmx jmx = new FakeJmx()
                .put("debezium.sql_server:type=connector-metrics,context=streaming,server=inv,task=0,database=a", "Connected", true)
                .put("debezium.sql_server:type=connector-metrics,context=streaming,server=inv,task=0,database=b", "Connected", true)
                .put("debezium.sql_server:type=connector-metrics,context=streaming,server=inv,task=1,database=c", "Connected", true);
        assertEquals(2, new DebeziumMetrics(jmx).read("inv", K, 0).streaming().size());
    }

    // Registered before the task's last start: the run before it.
    @Test
    void anMBeanOlderThanTheTaskIsStale() {
        FakeJmx jmx = new FakeJmx().put(SNAP, "SnapshotRunning", true).registeredAt(SNAP, 1_000);
        assertTrue(new DebeziumMetrics(jmx).read("inv", K, 2_000).snapshotStale());
        assertFalse(new DebeziumMetrics(jmx).read("inv", K, 500).snapshotStale());
    }
}
```

`DebeziumMapTest`:

```java
package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.wire.Backfill;
import io.turbolytics.turbostats.connect.wire.EventLag;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DebeziumMapTest {
    static final Instant NOW = Instant.parse("2026-10-05T10:00:00Z");

    static DebeziumMetrics.View view(List<Map<String, Object>> snap, boolean snapStale,
            List<Map<String, Object>> stream, boolean streamStale) {
        return new DebeziumMetrics.View(snap, snapStale, stream, streamStale);
    }

    // The spike's initial snapshot: running, no chunk, streaming not yet open.
    @Test
    void anInitialSnapshotBlocksTheStream() {
        Backfill b = DebeziumMetrics.backfill(view(List.of(Map.of("SnapshotRunning", true, "TotalTableCount", 3,
                "RemainingTableCount", 2, "SnapshotDurationInSeconds", 39L)), false, List.of(), false));
        assertEquals("running", b.state());
        assertTrue(b.blocksStream());
        assertEquals(3, b.unitsTotal());
        assertEquals(2, b.unitsLeft());
        assertEquals("table", b.unit());
    }

    // An incremental snapshot runs beside the stream: ChunkId is set.
    @Test
    void anIncrementalSnapshotDoesNotBlock() {
        Backfill b = DebeziumMetrics.backfill(view(List.of(Map.of("SnapshotRunning", true, "ChunkId", "6d0d")),
                false, List.of(), false));
        assertFalse(b.blocksStream());
    }

    @Test
    void missingOrStaleSnapshotMetricsAreUnknown() {
        assertEquals("unknown", DebeziumMetrics.backfill(view(List.of(), false, List.of(), false)).state());
        assertEquals("unknown", DebeziumMetrics.backfill(view(List.of(Map.of("SnapshotRunning", true)), true,
                List.of(), false)).state());
    }

    // Fresh after a completed snapshot: all zeros, neither flag set.
    @Test
    void noSnapshotThisRunIsNone() {
        assertEquals("none", DebeziumMetrics.backfill(view(List.of(Map.of("SnapshotRunning", false,
                "SnapshotCompleted", false)), false, List.of(), false)).state());
    }

    @Test
    void eventLagIsMillisecondsBehindSourceWithItsBasis() {
        EventLag l = DebeziumMetrics.eventLag(view(List.of(), false, List.of(Map.of("MilliSecondsBehindSource", 1500L,
                "MilliSecondsBehindSourceMaxValue", 9000L, "MilliSecondsSinceLastEvent", 2000L)), false), NOW);
        assertEquals(1.5, l.seconds());
        assertEquals(9.0, l.maxSeconds());
        assertEquals(NOW.minusMillis(2000), l.observedAt());
        assertEquals("source_commit_time", l.basis());
    }

    // Review focus: -1 means no event yet.
    @Test
    void noEventYetIsNoLag() {
        assertNull(DebeziumMetrics.eventLag(view(List.of(), false, List.of(Map.of("MilliSecondsBehindSource", -1L)),
                false), NOW));
    }

    // A database that is not yet streamed to cannot be lost.
    @Test
    void connectionIsAbsentWhileTheSnapshotBlocksOrWhenStale() {
        DebeziumMetrics.View v = view(List.of(), false, List.of(Map.of("Connected", false)), false);
        Backfill blocking = new Backfill("running", true, null, "table", null, null, null);
        assertNull(DebeziumMetrics.sourceConnected(v, blocking));
        assertFalse(DebeziumMetrics.sourceConnected(v, Backfill.none()));
        assertNull(DebeziumMetrics.sourceConnected(view(List.of(), false, List.of(Map.of("Connected", true)), true),
                Backfill.none()));
    }

    // Per-database MBeans are shards: lag is the worst, rows the sum.
    @Test
    void databasesCollapseAsShards() {
        EventLag l = DebeziumMetrics.eventLag(view(List.of(), false, List.of(
                Map.of("MilliSecondsBehindSource", 100L), Map.of("MilliSecondsBehindSource", 700L)), false), NOW);
        assertEquals(0.7, l.seconds());
    }
}
```

Run: `scripts/mvn -q test -Dtest='DebeziumMetricsTest,DebeziumMapTest'`
Expected: FAIL to compile with `cannot find symbol: class DebeziumMetrics`.

- [ ] **Step 2: Implement**

```java
package io.turbolytics.turbostats.connect.collect;

import io.turbolytics.turbostats.connect.wire.Backfill;
import io.turbolytics.turbostats.connect.wire.EventLag;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

/**
 * Debezium's metrics for one task. Debezium names them
 * debezium.<type>:type=connector-metrics,context=<snapshot|streaming>,
 * server=<topic.prefix>, adding task= for SQL Server and MongoDB and
 * database= per SQL Server database; without a task key they are task 0's.
 * One registered before the task's last start belongs to the run before
 * it: restarted mid-snapshot, a task's old MBeans stayed 80 s and the new
 * ones never registered.
 */
public final class DebeziumMetrics {
    static final String[] ATTRIBUTES = {"SnapshotRunning", "SnapshotPaused", "SnapshotCompleted", "SnapshotAborted",
        "ChunkId", "TotalTableCount", "RemainingTableCount", "RowsScanned", "SnapshotDurationInSeconds", "Connected",
        "MilliSecondsBehindSource", "MilliSecondsBehindSourceMaxValue", "MilliSecondsSinceLastEvent"};

    public record View(List<Map<String, Object>> snapshot, boolean snapshotStale,
            List<Map<String, Object>> streaming, boolean streamingStale) {
    }

    private final Jmx jmx;

    public DebeziumMetrics(Jmx jmx) {
        this.jmx = jmx;
    }

    public View read(String topicPrefix, TaskKey k, long taskStartMillis) {
        List<Map<String, Object>> snapshot = new ArrayList<>();
        List<Map<String, Object>> streaming = new ArrayList<>();
        boolean snapshotStale = false;
        boolean streamingStale = false;
        for (ObjectName n : jmx.query("debezium.*:type=connector-metrics,*")) {
            if (!topicPrefix.equals(ConnectMetrics.unquote(n.getKeyProperty("server")))) {
                continue;
            }
            String task = ConnectMetrics.unquote(n.getKeyProperty("task"));
            if (task == null ? k.task() != 0 : !task.equals(Integer.toString(k.task()))) {
                continue;
            }
            boolean stale = taskStartMillis > 0 && jmx.registeredAt(n) < taskStartMillis;
            Map<String, Object> values = new java.util.HashMap<>();
            for (String a : ATTRIBUTES) {
                Object v = jmx.attribute(n, a);
                if (v != null) {
                    values.put(a, v);
                }
            }
            switch (String.valueOf(n.getKeyProperty("context"))) {
                case "snapshot" -> {
                    snapshot.add(values);
                    snapshotStale |= stale;
                }
                case "streaming" -> {
                    streaming.add(values);
                    streamingStale |= stale;
                }
                default -> {
                }
            }
        }
        return new View(snapshot, snapshotStale, streaming, streamingStale);
    }

    /** Running while any shard runs; unknown without current metrics. */
    public static Backfill backfill(View v) {
        if (v.snapshot().isEmpty() || v.snapshotStale()) {
            return Backfill.unknown();
        }
        boolean running = any(v.snapshot(), "SnapshotRunning");
        boolean paused = any(v.snapshot(), "SnapshotPaused");
        boolean aborted = any(v.snapshot(), "SnapshotAborted");
        boolean completed = all(v.snapshot(), "SnapshotCompleted");
        String state = running ? (paused ? "paused" : "running") : aborted ? "aborted" : completed ? "completed" : "none";
        if (state.equals("none")) {
            return Backfill.none();
        }
        boolean incremental = v.snapshot().stream().anyMatch(m -> m.get("ChunkId") != null);
        return new Backfill(state, (running || paused) && !incremental,
                max(v.snapshot(), "SnapshotDurationInSeconds"), "table",
                intOrNull(sum(v.snapshot(), "TotalTableCount")), intOrNull(sum(v.snapshot(), "RemainingTableCount")),
                rows(v.snapshot()));
    }

    /** The worst shard's lag; absent before the first event (-1). */
    public static EventLag eventLag(View v, Instant now) {
        if (v.streaming().isEmpty() || v.streamingStale()) {
            return null;
        }
        Long behind = max(v.streaming(), "MilliSecondsBehindSource");
        if (behind == null) {
            return null;
        }
        Long worst = max(v.streaming(), "MilliSecondsBehindSourceMaxValue");
        Long since = min(v.streaming(), "MilliSecondsSinceLastEvent");
        return new EventLag(behind / 1000.0, worst == null ? null : worst / 1000.0,
                since == null ? now : now.minusMillis(since), "source_commit_time");
    }

    /** Absent while a blocking snapshot runs, before the stream connects, and when stale. */
    public static Boolean sourceConnected(View v, Backfill b) {
        if (v.streaming().isEmpty() || v.streamingStale()) {
            return null;
        }
        if (b != null && b.blocksStream() && (b.state().equals("running") || b.state().equals("paused"))) {
            return null;
        }
        return v.streaming().stream().allMatch(m -> Boolean.TRUE.equals(m.get("Connected")));
    }

    /** Now less the fewest milliseconds since any context's last event. */
    public static Instant lastEvent(View v, Instant now) {
        List<Map<String, Object>> all = new ArrayList<>();
        if (!v.snapshotStale()) {
            all.addAll(v.snapshot());
        }
        if (!v.streamingStale()) {
            all.addAll(v.streaming());
        }
        Long since = min(all, "MilliSecondsSinceLastEvent");
        return since == null ? null : now.minusMillis(since);
    }

    private static boolean any(List<Map<String, Object>> ms, String a) {
        return ms.stream().anyMatch(m -> Boolean.TRUE.equals(m.get(a)));
    }

    private static boolean all(List<Map<String, Object>> ms, String a) {
        return !ms.isEmpty() && ms.stream().allMatch(m -> Boolean.TRUE.equals(m.get(a)));
    }

    /** Non-negative values only: Debezium uses -1 for "none yet". */
    private static Long max(List<Map<String, Object>> ms, String a) {
        Long out = null;
        for (Map<String, Object> m : ms) {
            if (m.get(a) instanceof Number n && n.longValue() >= 0) {
                out = out == null ? n.longValue() : Math.max(out, n.longValue());
            }
        }
        return out;
    }

    private static Long min(List<Map<String, Object>> ms, String a) {
        Long out = null;
        for (Map<String, Object> m : ms) {
            if (m.get(a) instanceof Number n && n.longValue() >= 0) {
                out = out == null ? n.longValue() : Math.min(out, n.longValue());
            }
        }
        return out;
    }

    private static Long sum(List<Map<String, Object>> ms, String a) {
        Long out = null;
        for (Map<String, Object> m : ms) {
            if (m.get(a) instanceof Number n && n.longValue() >= 0) {
                out = (out == null ? 0 : out) + n.longValue();
            }
        }
        return out;
    }

    private static Integer intOrNull(Long v) {
        return v == null ? null : (int) Math.min(Integer.MAX_VALUE, v);
    }

    /** RowsScanned is a table-to-count map, exposed as JMX tabular data. */
    private static Long rows(List<Map<String, Object>> ms) {
        Long out = null;
        for (Map<String, Object> m : ms) {
            if (m.get("RowsScanned") instanceof TabularData t) {
                for (Object row : t.values()) {
                    if (row instanceof CompositeData c && c.get("value") instanceof Number n) {
                        out = (out == null ? 0 : out) + n.longValue();
                    }
                }
            }
        }
        return out;
    }
}
```

- [ ] **Step 3: Run the tests and commit**

Run: `scripts/mvn -q test -Dtest='DebeziumMetricsTest,DebeziumMapTest'`
Expected: PASS.

```bash
git add src && git commit -m "collect: read Debezium's snapshot and streaming metrics for a task, stale ones marked"
```

---

### Task 4: Debezium fields in the source bundle

**Files:**
- Modify: `src/main/java/.../collect/TaskCollector.java`, `src/main/java/.../collect/ConnectMetrics.java` (a client-bytes reader)
- Test: `src/test/java/.../collect/TaskCollectorTest.java`

**Interfaces:**
- Consumes: `DebeziumMetrics` (Task 3), `Jmx.registeredAt` (Task 2).
- Produces: `TaskCollector` builds a `DebeziumMetrics` from the same `Jmx` it reads Connect through, so the constructor takes `Jmx jmx` in place of `ConnectMetrics metrics` and builds both. `ConnectMetrics.clientBytes(String domain, String type, String clientId, String attribute): OptionalLong`.

- [ ] **Step 1: Write the failing tests**

Add to `TaskCollectorTest` (its `collector()` passes `jmx` where it passed `new ConnectMetrics(jmx)`):

```java
    static final String DBZ_SNAP = "debezium.postgres:type=connector-metrics,context=snapshot,server=inventory";
    static final String DBZ_STREAM = "debezium.postgres:type=connector-metrics,context=streaming,server=inventory";

    // The Cluster fake's config gives the source connector topic.prefix
    // "inventory" and a Debezium class, so its metrics are found.
    @Test
    void aDebeziumSourceReportsBackfillLagAndConnection() {
        runningSource(10, 10, 0);
        jmx.put(DBZ_SNAP, "SnapshotCompleted", true).put(DBZ_SNAP, "TotalTableCount", 1)
                .put(DBZ_SNAP, "RemainingTableCount", 0)
                .put(DBZ_STREAM, "Connected", true).put(DBZ_STREAM, "MilliSecondsBehindSource", 250L)
                .put(DBZ_STREAM, "MilliSecondsSinceLastEvent", 5000L)
                .put("kafka.producer:type=producer-metrics,client-id=connector-producer-inventory-cdc-0",
                        "outgoing-byte-total", 4096.0);
        Bundle b = collector().collect(NOW).get(0);
        assertEquals("completed", b.pipeline().backfill().state());
        assertEquals(0.25, b.pipeline().eventLag().seconds());
        assertEquals(Boolean.TRUE, b.pipeline().sourceConnected());
        assertEquals(NOW.minusMillis(5000), b.pipeline().lastMessageAt());
        assertEquals(4096L, b.pipeline().sinkWireBytes());
    }

    // A Debezium source always sends backfill; without metrics it is unknown.
    @Test
    void aDebeziumSourceWithoutMetricsSaysUnknown() {
        runningSource(10, 10, 0);
        Bundle b = collector().collect(NOW).get(0);
        assertEquals("unknown", b.pipeline().backfill().state());
        assertNull(b.pipeline().eventLag());
        assertNull(b.pipeline().sourceConnected());
    }
```

and change the `Cluster` fake's source config to:

```java
            return connector.equals("customers-sink")
                    ? Map.of("connector.class", "io.debezium.connector.jdbc.JdbcSinkConnector")
                    : Map.of("connector.class", "io.debezium.connector.postgresql.PostgresConnector",
                            "topic.prefix", "inventory");
```

Run: `scripts/mvn -q test -Dtest=TaskCollectorTest`
Expected: FAIL: the new tests find `backfill()` null.

- [ ] **Step 2: Implement**

In `TaskCollector`:
- `ConnectorFacts` gains `String topicPrefix, boolean debezium`, set from the config: `topic.prefix`, or `database.server.name` for older Debezium, and `connector.class` starting with `io.debezium.connector.` on a source connector.
- For a source task whose facts say Debezium:

```java
            DebeziumMetrics.View view = debezium.read(f.topicPrefix(), k, starts > 0 ? startMillis : 0);
            backfill = DebeziumMetrics.backfill(view);
            eventLag = DebeziumMetrics.eventLag(view, now);
            sourceConnected = DebeziumMetrics.sourceConnected(view, backfill);
            Instant last = DebeziumMetrics.lastEvent(view, now);
            if (last != null) {
                lastMessage = last;
            }
```

- For every source task, `sinkWireBytes = metrics.clientBytes("kafka.producer", "producer-metrics", "connector-producer-" + k.connector() + "-" + k.task(), "outgoing-byte-total")`, null when empty.

`ConnectMetrics.clientBytes` queries `<domain>:type=<type>,*`, picks the name whose unquoted `client-id` equals the client id and which has no `topic` key, and reads the attribute as `total` does.

- [ ] **Step 3: Run the tests and commit**

Run: `scripts/mvn -q test -Dtest='TaskCollectorTest,BundleSchemaTest'`
Expected: PASS.

```bash
git add src && git commit -m "collect: a Debezium source reports backfill, event lag, connection and wire bytes"
```

---

### Task 5: Sink lag from the broker

**Files:**
- Create: `src/main/java/.../collect/GroupAdmin.java`, `KafkaGroupAdmin.java`, `BrokerLag.java`, `AdminSettings.java`
- Test: `src/test/java/.../collect/BrokerLagTest.java`, `AdminSettingsTest.java`

**Interfaces:**
- Produces:
  - `interface GroupAdmin extends AutoCloseable` with `Map<String, Set<TopicPartition>> assignments(String group, Duration timeout)`, `Map<TopicPartition, Long> committed(String group, Duration timeout)`, `Map<TopicPartition, Long> ends(Set<TopicPartition> tps, Duration timeout)`, all throwing `Exception`.
  - `KafkaGroupAdmin(Properties)` over `org.apache.kafka.clients.admin.Admin`.
  - `AdminSettings.from(Map<String, ?> worker): Properties`.
  - `BrokerLag(Supplier<GroupAdmin> admin, Duration timeout)` with `Map<TaskKey, MessageLag> lags(Map<String, String> groupByConnector, Instant now)` and `void close()`.

- [ ] **Step 1: Write the failing tests**

`AdminSettingsTest`:

```java
package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class AdminSettingsTest {
    // The worker's connection and security settings, with admin.* overrides,
    // and nothing else: the worker's other keys would only be logged as unused.
    @Test
    void keepsConnectionAndSecuritySettingsOnly() {
        Properties p = AdminSettings.from(Map.of("bootstrap.servers", "kafka:9092", "security.protocol", "SASL_SSL",
                "sasl.mechanism", "PLAIN", "ssl.truststore.location", "/t", "admin.request.timeout.ms", "5000",
                "group.id", "connect", "turbostats.key", "secret"));
        assertEquals("kafka:9092", p.get("bootstrap.servers"));
        assertEquals("SASL_SSL", p.get("security.protocol"));
        assertEquals("PLAIN", p.get("sasl.mechanism"));
        assertEquals("/t", p.get("ssl.truststore.location"));
        assertEquals("5000", p.get("request.timeout.ms"));
        assertFalse(p.containsKey("group.id"));
        assertFalse(p.containsKey("turbostats.key"));
        assertEquals("turbostats-reporter", p.get("client.id"));
    }
}
```

`BrokerLagTest`:

```java
package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.wire.MessageLag;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class BrokerLagTest {
    static final Instant NOW = Instant.parse("2026-10-05T10:00:00Z");
    static final TopicPartition P0 = new TopicPartition("t", 0);
    static final TopicPartition P1 = new TopicPartition("t", 1);
    static final TopicPartition P2 = new TopicPartition("t", 2);

    static class Fake implements GroupAdmin {
        Map<String, Set<TopicPartition>> members = new HashMap<>();
        Map<TopicPartition, Long> committed = new HashMap<>();
        Map<TopicPartition, Long> ends = new HashMap<>();
        boolean timeout;

        public Map<String, Set<TopicPartition>> assignments(String group, Duration t) throws Exception {
            if (timeout) {
                throw new TimeoutException("broker");
            }
            return members;
        }

        public Map<TopicPartition, Long> committed(String group, Duration t) {
            return committed;
        }

        public Map<TopicPartition, Long> ends(Set<TopicPartition> tps, Duration t) {
            return ends;
        }

        public void close() {
        }
    }

    // The spike's two-task sink: task 0 held partitions 0 and 1, task 1 held 2.
    @Test
    void eachTaskGetsItsPartitionsLag() {
        Fake f = new Fake();
        f.members.put("connector-consumer-s-0", Set.of(P0, P1));
        f.members.put("connector-consumer-s-1", Set.of(P2));
        f.committed.putAll(Map.of(P0, 90L, P1, 100L, P2, 40L));
        f.ends.putAll(Map.of(P0, 100L, P1, 130L, P2, 40L));
        Map<TaskKey, MessageLag> lags = new BrokerLag(() -> f, Duration.ofSeconds(1)).lags(Map.of("s", "connect-s"), NOW);
        assertEquals(new MessageLag(30, 40, 2, NOW), lags.get(new TaskKey("s", 0)));
        assertEquals(new MessageLag(0, 0, 1, NOW), lags.get(new TaskKey("s", 1)));
    }

    // Review focus: a broker that times out leaves lag absent this tick.
    @Test
    void aTimeoutLeavesLagAbsent() {
        Fake f = new Fake();
        f.timeout = true;
        assertTrue(new BrokerLag(() -> f, Duration.ofSeconds(1)).lags(Map.of("s", "connect-s"), NOW).isEmpty());
    }

    // Review focus: no commit yet means unknown lag, not zero or the topic.
    @Test
    void aPartitionWithoutACommitIsLeftOut() {
        Fake f = new Fake();
        f.members.put("connector-consumer-s-0", Set.of(P0, P1));
        f.committed.put(P0, 5L);
        f.ends.putAll(Map.of(P0, 10L, P1, 1000L));
        assertEquals(new MessageLag(5, 5, 1, NOW),
                new BrokerLag(() -> f, Duration.ofSeconds(1)).lags(Map.of("s", "connect-s"), NOW).get(new TaskKey("s", 0)));
    }
}
```

Run: `scripts/mvn -q test -Dtest='BrokerLagTest,AdminSettingsTest'`
Expected: FAIL to compile.

- [ ] **Step 2: Implement**

`AdminSettings.from`: copy `bootstrap.servers`, `security.protocol`, `client.dns.lookup`, and keys starting `sasl.` or `ssl.`; then every `admin.`-prefixed key with the prefix stripped; then `client.id=turbostats-reporter`. Values are `String.valueOf`.

`GroupAdmin` as in Interfaces. `KafkaGroupAdmin`:

```java
    public Map<String, Set<TopicPartition>> assignments(String group, Duration t) throws Exception {
        ConsumerGroupDescription d = admin.describeConsumerGroups(List.of(group)).all()
                .get(t.toMillis(), TimeUnit.MILLISECONDS).get(group);
        Map<String, Set<TopicPartition>> out = new HashMap<>();
        for (MemberDescription m : d.members()) {
            out.computeIfAbsent(m.clientId(), x -> new HashSet<>()).addAll(m.assignment().topicPartitions());
        }
        return out;
    }

    public Map<TopicPartition, Long> committed(String group, Duration t) throws Exception {
        Map<TopicPartition, Long> out = new HashMap<>();
        admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(t.toMillis(), TimeUnit.MILLISECONDS)
                .forEach((tp, om) -> {
                    if (om != null) {
                        out.put(tp, om.offset());
                    }
                });
        return out;
    }

    public Map<TopicPartition, Long> ends(Set<TopicPartition> tps, Duration t) throws Exception {
        Map<TopicPartition, OffsetSpec> spec = new HashMap<>();
        tps.forEach(tp -> spec.put(tp, OffsetSpec.latest()));
        Map<TopicPartition, Long> out = new HashMap<>();
        admin.listOffsets(spec).all().get(t.toMillis(), TimeUnit.MILLISECONDS).forEach((tp, r) -> out.put(tp, r.offset()));
        return out;
    }
```

`BrokerLag.lags`: create the admin from the supplier on first use and keep it; for each connector, call the three methods inside one `try`; on any exception, skip that connector (and close and drop the admin when the exception is not a timeout, so a broken client is rebuilt next tick); for each member whose `client.id` parses to a `TaskKey`, sum and max `ends - committed` over its partitions that have both, clamped at zero, and count them; a member with no counted partition gets no entry.

- [ ] **Step 3: Run the tests and commit**

Run: `scripts/mvn -q test -Dtest='BrokerLagTest,AdminSettingsTest'`
Expected: PASS.

```bash
git add src && git commit -m "collect: sink lag in messages from the broker, per task"
```

---

### Task 6: Sink event lag and wire bytes

**Files:**
- Modify: `src/main/java/.../intercept/TaskCounters.java`, `ConsumeInterceptor.java`, `src/main/java/.../collect/TaskCollector.java`
- Test: `src/test/java/.../intercept/ConsumeLagTest.java`, `src/test/java/.../collect/TaskCollectorTest.java`

**Interfaces:**
- Produces: `TaskCounters.Snapshot` gains `long eventLagMillis` (-1 none), `long maxEventLagMillis`, `long eventObservedMillis`, `String timestampBasis`. `TaskCollector` takes a `BrokerLag` and, once per tick, asks it for every local sink connector's group: `consumer.override.group.id` from the connector config, else `connect-<connector>`.

- [ ] **Step 1: Write the failing tests**

`ConsumeLagTest`:

```java
package io.turbolytics.turbostats.connect.intercept;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.collect.TaskKey;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConsumeLagTest {
    static final TaskKey K = new TaskKey("s", 0);

    @BeforeEach
    void reset() {
        TaskCounters.clear();
    }

    static ConsumerRecord<Object, Object> record(long ts, TimestampType type) {
        return new ConsumerRecord<>("t", 0, 0L, ts, type, -1, -1, "k", "v", new RecordHeaders(), Optional.empty());
    }

    static ConsumeInterceptor sink() {
        ConsumeInterceptor c = new ConsumeInterceptor();
        c.configure(Map.of("client.id", "connector-consumer-s-0"));
        return c;
    }

    // Now less the newest record in the batch, with the timestamp's type.
    @Test
    void eachBatchMeasuresItsNewestRecord() {
        long now = System.currentTimeMillis();
        sink().onConsume(new ConsumerRecords<>(Map.of(new TopicPartition("t", 0), List.of(
                record(now - 9000, TimestampType.CREATE_TIME), record(now - 2000, TimestampType.CREATE_TIME)))));
        TaskCounters.Snapshot s = TaskCounters.find(K).orElseThrow().snapshot();
        assertTrue(s.eventLagMillis() >= 2000 && s.eventLagMillis() < 3000, "lag " + s.eventLagMillis());
        assertEquals("kafka_create_time", s.timestampBasis());
        assertTrue(s.eventObservedMillis() >= now);
    }

    // Review focus: no timestamp, no lag.
    @Test
    void recordsWithoutTimestampsGiveNoLag() {
        sink().onConsume(new ConsumerRecords<>(Map.of(new TopicPartition("t", 0), List.of(
                record(-1, TimestampType.NO_TIMESTAMP_TYPE)))));
        assertEquals(-1, TaskCounters.find(K).orElseThrow().snapshot().eventLagMillis());
    }
}
```

Add to `TaskCollectorTest` (its `collector()` passes a `BrokerLag` over a `BrokerLagTest.Fake`-like stub that returns `new MessageLag(7, 9, 2, NOW)` for `customers-sink/0`):

```java
    @Test
    void aSinkReportsBrokerLagEventLagAndWireBytes() {
        jmx.put(SNK_TASK, "status", "running")
                .put(SNK_METRICS, "sink-record-read-total", 10.0)
                .put(SNK_METRICS, "sink-record-send-total", 10.0)
                .put("kafka.consumer:type=consumer-fetch-manager-metrics,client-id=connector-consumer-customers-sink-0",
                        "bytes-consumed-total", 2048.0);
        cluster.tasks.put(SNK, new TaskHealth("RUNNING", "10.0.3.8:8083", "sink"));
        cluster.connectorStates.put("customers-sink", "RUNNING");
        Bundle b = collector().collect(NOW).get(0);
        assertEquals(7, b.pipeline().messageLag().maxMessages());
        assertEquals(2048L, b.pipeline().sourceWireBytes());
    }
```

Run: `scripts/mvn -q test -Dtest='ConsumeLagTest,TaskCollectorTest'`
Expected: FAIL to compile.

- [ ] **Step 2: Implement**

`TaskCounters`: `volatile long eventLagMillis = -1`, `maxEventLagMillis`, `eventObservedMillis`, `volatile String timestampBasis`; `void event(long lagMillis, long nowMillis, String basis)` sets them and raises the max; `consumerStarted` resets them. `ConsumeInterceptor.onConsume` finds the newest timestamp and its type in the batch in one pass, and when it is not negative records `now - newest` with `kafka_create_time` for `CREATE_TIME` and `kafka_log_append_time` for `LOG_APPEND_TIME`.

`TaskCollector`: for a sink task, `eventLag` from the snapshot when `eventLagMillis >= 0` (`observedAt` = `eventObservedMillis`), `messageLag` from this tick's `BrokerLag` answer, `sourceWireBytes` from `clientBytes("kafka.consumer", "consumer-fetch-manager-metrics", "connector-consumer-<c>-<t>", "bytes-consumed-total")`.

`TurboStatsExtension.configure` keeps the worker properties; `register` builds `new BrokerLag(() -> new KafkaGroupAdmin(AdminSettings.from(worker)), Duration.ofSeconds(cfg.timeoutSeconds()))` and `close` closes it.

- [ ] **Step 3: Run the tests and commit**

Run: `scripts/mvn -q test -Dtest='ConsumeLagTest,TaskCollectorTest,TurboStatsExtensionTest'`
Expected: PASS.

```bash
git add src && git commit -m "collect: a sink reports broker lag, event lag and wire bytes"
```

---

### Task 7: Integration and release tests

**Files:**
- Create: `src/test/java/.../collect/BrokerLagIntegrationIT.java`
- Modify: `src/test/java/.../connect/WorkerReleaseIT.java`

- [ ] **Step 1: Broker lag against a real broker**

`BrokerLagIntegrationIT`: start a `KafkaContainer`; create topic `t` with two partitions; produce 10 records to each; start two consumers in group `connect-it` with client ids `connector-consumer-it-0` and `connector-consumer-it-1`, poll until both hold one partition, commit offset 4 on each; then:

```java
        Map<TaskKey, MessageLag> lags = new BrokerLag(
                () -> new KafkaGroupAdmin(AdminSettings.from(Map.of("bootstrap.servers", kafka.getBootstrapServers()))),
                Duration.ofSeconds(10)).lags(Map.of("it", "connect-it"), Instant.now());
        assertEquals(6, lags.get(new TaskKey("it", 0)).totalMessages());
        assertEquals(6, lags.get(new TaskKey("it", 1)).totalMessages());
```

- [ ] **Step 2: Release assertions**

In `WorkerReleaseIT.aRunningConnectorReportsSignedValidBundles`, after the existing assertions, insert 100 rows and await a post for `itest/inventory-cdc/0` with `pipeline.event_lag_basis` = `source_commit_time`; assert `backfill.state` = `completed`, `backfill.units_total` = 1, `source_connected` true, `sink_wire_bytes` > 0, and that the post validates against the schema.

Add `aSinkReportsLagFromTheBroker`: create a Debezium JDBC sink reading `inventory-cdc.public.customers` into `customers_copy` (the spike's sink config), await a post for `itest/customers-sink/0` with `lag_partitions` ≥ 1, `lag_observed_at` present, `event_lag_basis` = `kafka_create_time`, and `source_wire_bytes` > 0.

- [ ] **Step 3: Run the targeted layers and commit**

Run: `scripts/mvn verify -Pintegration -Dit.test=BrokerLagIntegrationIT`
Expected: PASS. The release layer runs in CI.

```bash
git add src/test && git commit -m "test: broker lag against a real broker, and Debezium and sink fields in the release test"
```

---

### Task 8: README, PR, release

- [ ] **Step 1:** In `README.md`'s "What a report carries", add: event lag (Debezium: commit time to processing; sinks: record timestamp to processing), snapshot progress, the database connection, sink lag in messages from the broker, and wire bytes. State that the reporter's admin client connects with the worker's own connection settings, and that a task restarted mid-snapshot reports `backfill.state: unknown` because Debezium does not register its new snapshot metrics.

- [ ] **Step 2:** Push the branch, open the PR, run one bounded review (10 minutes, 25 calls, three questions: stale-MBean handling, admin-client lifecycle and timeouts, absent-versus-zero on the new fields). Tag `v0.2.0` after merge.
