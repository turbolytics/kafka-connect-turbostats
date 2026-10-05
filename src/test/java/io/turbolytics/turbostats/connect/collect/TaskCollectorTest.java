package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.config.ReporterConfig;
import io.turbolytics.turbostats.connect.intercept.AckInterceptor;
import io.turbolytics.turbostats.connect.intercept.TaskCounters;
import io.turbolytics.turbostats.connect.wire.Bundle;
import io.turbolytics.turbostats.connect.wire.BundleSchemaTest;
import io.turbolytics.turbostats.connect.wire.Fixtures;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TaskCollectorTest {
    static final TaskKey SRC = new TaskKey("inventory-cdc", 0);
    static final TaskKey SNK = new TaskKey("customers-sink", 0);
    static final Instant NOW = Instant.parse("2026-10-04T10:00:00Z");
    static final String SRC_TASK = "kafka.connect:type=connector-task-metrics,connector=inventory-cdc,task=0";
    static final String SRC_METRICS = "kafka.connect:type=source-task-metrics,connector=inventory-cdc,task=0";
    static final String SNK_TASK = "kafka.connect:type=connector-task-metrics,connector=customers-sink,task=0";
    static final String SNK_METRICS = "kafka.connect:type=sink-task-metrics,connector=customers-sink,task=0";

    static class Cluster implements ClusterView {
        final Map<TaskKey, TaskHealth> tasks = new HashMap<>();
        final Map<String, String> connectorStates = new HashMap<>();
        final java.util.Set<String> unreachable = new java.util.HashSet<>();
        final java.util.Set<TaskKey> throwing = new java.util.HashSet<>();
        boolean configUnavailable;

        @Override
        public Optional<TaskHealth> task(TaskKey k) {
            if (throwing.contains(k)) {
                throw new IllegalStateException("boom");
            }
            return Optional.ofNullable(tasks.get(k));
        }

        @Override
        public ConnectorLookup connector(String connector) {
            if (unreachable.contains(connector)) {
                return ConnectorLookup.unknown();
            }
            String state = connectorStates.get(connector);
            return state == null ? ConnectorLookup.notFound() : ConnectorLookup.of(state);
        }

        @Override
        public Map<String, String> config(String connector) {
            if (configUnavailable) {
                return Map.of();
            }
            return connector.equals("customers-sink")
                    ? Map.of("connector.class", "io.debezium.connector.jdbc.JdbcSinkConnector")
                    : Map.of("connector.class", "io.debezium.connector.postgresql.PostgresConnector",
                            "topic.prefix", "inventory");
        }
    }

    FakeJmx jmx;
    Cluster cluster;
    List<String> warnings;

    @BeforeEach
    void setUp() {
        TaskCounters.clear();
        jmx = new FakeJmx();
        cluster = new Cluster();
        warnings = new ArrayList<>();
    }

    TaskCollector collector() {
        Map<String, Object> props = new HashMap<>();
        props.put("group.id", "prod-connect");
        props.put("turbostats.report.to", "https://control.turbolytics.io/v1/turbostats");
        props.put("turbostats.key", "sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8");
        ReporterConfig cfg = ReporterConfig.parse(props).config();
        return new TaskCollector(cfg, jmx, cluster, () -> Fixtures.process().withHost(null),
                "3.9.0", warnings::add);
    }

    void runningSource(long poll, long write, long active) {
        jmx.put(SRC_TASK, "status", "running")
                .put(SRC_METRICS, "source-record-poll-total", (double) poll)
                .put(SRC_METRICS, "source-record-write-total", (double) write)
                .put(SRC_METRICS, "source-record-active-count", (double) active);
        cluster.tasks.put(SRC, new TaskHealth("RUNNING", "10.0.3.7:8083", "source"));
        cluster.connectorStates.put("inventory-cdc", "RUNNING");
    }

    static final String DBZ_SNAP = "debezium.postgres:type=connector-metrics,context=snapshot,server=inventory";
    static final String DBZ_STREAM = "debezium.postgres:type=connector-metrics,context=streaming,server=inventory";

    // The Cluster fake gives the source connector topic.prefix "inventory"
    // and a Debezium class, so its metrics are found.
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
        assertNull(b.pipeline().sinkWireBytes());
    }

    // Review focus: a worker with no tasks sends nothing.
    @Test
    void aWorkerWithNoTasksSendsNothing() {
        assertEquals(List.of(), collector().collect(NOW));
    }

    // Review focus: a starting or moving task has no metrics yet. Zeros
    // would claim it has done nothing; it is skipped until it has some.
    @Test
    void aTaskWithoutMetricsIsSkipped() {
        jmx.put(SRC_TASK, "status", "running");
        cluster.tasks.put(SRC, new TaskHealth("RUNNING", "10.0.3.7:8083", "source"));
        assertEquals(List.of(), collector().collect(NOW));
    }

    @Test
    void aSourceTaskReportsIdentityStateAndAcknowledgedWrites() {
        new AckInterceptor().configure(Map.of("client.id", "connector-producer-inventory-cdc-0"));
        AckInterceptor ack = new AckInterceptor();
        ack.configure(Map.of("client.id", "connector-producer-inventory-cdc-0"));
        for (int i = 0; i < 50; i++) {
            ack.onSend(null);
        }
        for (int i = 0; i < 40; i++) {
            ack.onAcknowledgement(null, null);
        }
        runningSource(50, 40, 10);

        Bundle b = collector().collect(NOW).get(0);
        assertEquals("prod-connect/inventory-cdc/0", b.instance().id());
        assertEquals("prod-connect/inventory-cdc", b.instance().name());
        assertEquals("postgres", b.instance().sourceType());
        assertEquals("kafka", b.instance().sinkType());
        assertEquals("kafka-connect", b.instance().runtime());
        assertEquals("10.0.3.7:8083", b.process().host());
        assertEquals("running", b.pipeline().state());
        assertEquals(50, b.pipeline().messageCount());
        // Accepted is what the task handed the producer; written is what the
        // broker acknowledged.
        assertEquals(50, b.pipeline().sinkRowsAccepted());
        assertEquals(40, b.pipeline().sinkRowsWritten());
        assertNull(b.pipeline().sinkFlushCount());
        assertEquals(0, b.pipeline().stateCommitCount());
        assertEquals(1L, b.pipeline().restartCount());
        assertTrue(b.pipeline().lastSinkWriteAt() != null);
    }

    // Without the interceptor's counts, written is Connect's write total,
    // which counts records the broker acknowledged (Connect 3.9 records it in
    // the producer callback). Subtracting records in flight would count them
    // twice. There is no acknowledgment time to report.
    @Test
    void withoutTheInterceptorWrittenFallsBackToConnectsCounts() {
        runningSource(50, 40, 10);
        Bundle b = collector().collect(NOW).get(0);
        assertEquals(40, b.pipeline().sinkRowsWritten());
        assertEquals(40, b.pipeline().sinkRowsAccepted());
        assertNull(b.pipeline().lastSinkWriteAt());
        assertNull(b.pipeline().restartCount());
        assertNull(b.pipeline().startedAt());
    }

    // The spike once saw acknowledgments stop while writes rose. Writes
    // rising, acknowledgments flat and nothing in flight means the counter
    // is broken: fall back and say so once.
    @Test
    void aStalledAcknowledgmentCountFallsBackAndWarnsOnce() {
        AckInterceptor ack = new AckInterceptor();
        ack.configure(Map.of("client.id", "connector-producer-inventory-cdc-0"));
        ack.onAcknowledgement(null, null);
        TaskCollector c = collector();
        runningSource(10, 10, 9);
        c.collect(NOW);
        runningSource(30, 30, 0);
        Bundle b = c.collect(NOW.plusSeconds(60)).get(0);
        assertEquals(30, b.pipeline().sinkRowsWritten());
        assertNull(b.pipeline().lastSinkWriteAt());
        // Still broken while acknowledgments stay flat: no fall back to the
        // stale count of 1, and no second warning.
        assertEquals(30, c.collect(NOW.plusSeconds(120)).get(0).pipeline().sinkRowsWritten());
        assertEquals(1, warnings.size());
    }

    @Test
    void aSinkTaskCountsWrittenAsReadLessInFlightAndFlushesAsCommits() {
        jmx.put(SNK_TASK, "status", "running")
                .put(SNK_METRICS, "sink-record-read-total", 1000.0)
                .put(SNK_METRICS, "sink-record-send-total", 1000.0)
                .put(SNK_METRICS, "sink-record-active-count", 274.0)
                .put(SNK_METRICS, "offset-commit-completion-total", 12.0);
        cluster.tasks.put(SNK, new TaskHealth("RUNNING", "10.0.3.8:8083", "sink"));
        cluster.connectorStates.put("customers-sink", "RUNNING");

        Bundle b = collector().collect(NOW).get(0);
        assertEquals("kafka", b.instance().sourceType());
        assertEquals("jdbc", b.instance().sinkType());
        assertEquals(1000, b.pipeline().messageCount());
        assertEquals(726, b.pipeline().sinkRowsWritten());
        assertNull(b.pipeline().sinkFlushCount());
    }

    // Connect raises offset-commit-completion-total even when it skips a
    // commit because nothing changed, as it does every flush interval while
    // a sink's destination is down. Only rows finishing is a write.
    @Test
    void aSinkCommitWithoutNewRowsIsNotAWrite() {
        jmx.put(SNK_TASK, "status", "running")
                .put(SNK_METRICS, "sink-record-read-total", 1000.0)
                .put(SNK_METRICS, "sink-record-send-total", 1000.0)
                .put(SNK_METRICS, "sink-record-active-count", 1000.0)
                .put(SNK_METRICS, "offset-commit-completion-total", 12.0);
        cluster.tasks.put(SNK, new TaskHealth("RUNNING", "10.0.3.8:8083", "sink"));
        cluster.connectorStates.put("customers-sink", "RUNNING");
        TaskCollector c = collector();
        c.collect(NOW);
        jmx.put(SNK_METRICS, "offset-commit-completion-total", 30.0);
        Bundle stuck = c.collect(NOW.plusSeconds(60)).get(0);
        assertNull(stuck.pipeline().lastSinkWriteAt());
        assertNull(stuck.pipeline().sinkFlushCount());

        jmx.put(SNK_METRICS, "sink-record-active-count", 0.0);
        Bundle wrote = c.collect(NOW.plusSeconds(120)).get(0);
        assertEquals(NOW.plusSeconds(120), wrote.pipeline().lastSinkWriteAt());
    }

    // A failed task on a live worker is the failure this reporter exists for.
    @Test
    void connectStatesMapToTheContractsStates() {
        runningSource(1, 1, 0);
        for (String[] pair : new String[][] {{"RUNNING", "running"}, {"PAUSED", "paused"},
                {"FAILED", "failed"}, {"RESTARTING", "starting"}}) {
            cluster.tasks.put(SRC, new TaskHealth(pair[0], "10.0.3.7:8083", "source"));
            assertEquals(pair[1], collector().collect(NOW).get(0).pipeline().state());
        }
        cluster.tasks.put(SRC, new TaskHealth("UNASSIGNED", "10.0.3.7:8083", "source"));
        assertEquals(List.of(), collector().collect(NOW));
    }

    // A deleted connector's tasks vanish; its last bundle goes out once
    // more, stopped, with an exit. A task that moved sends nothing.
    @Test
    void aDeletedConnectorSendsAnExitAndAMovedTaskSendsNothing() {
        runningSource(5, 5, 0);
        TaskCollector c = collector();
        c.collect(NOW);

        jmx.remove(SRC_TASK).remove(SRC_METRICS);
        cluster.tasks.remove(SRC);
        cluster.connectorStates.remove("inventory-cdc");
        List<Bundle> out = c.collect(NOW.plusSeconds(60));
        assertEquals(1, out.size());
        assertEquals("connector_deleted", out.get(0).exit().reason());
        assertEquals("stopped", out.get(0).pipeline().state());
        assertEquals(List.of(), c.collect(NOW.plusSeconds(120)));

        runningSource(5, 5, 0);
        TaskCollector moved = collector();
        moved.collect(NOW);
        jmx.remove(SRC_TASK).remove(SRC_METRICS);
        assertEquals(List.of(), moved.collect(NOW.plusSeconds(60)));
    }

    @Test
    void aStoppedConnectorSendsAnExit() {
        runningSource(5, 5, 0);
        TaskCollector c = collector();
        c.collect(NOW);
        jmx.remove(SRC_TASK).remove(SRC_METRICS);
        cluster.connectorStates.put("inventory-cdc", "STOPPED");
        assertEquals("connector_stopped", c.collect(NOW.plusSeconds(60)).get(0).exit().reason());
    }

    // The status store can say RUNNING, with the task still listed here, for
    // a tick after the task stopped. The exit waits for a definite answer
    // rather than being decided once and lost.
    @Test
    void anExitWaitsForTheStatusToCatchUp() {
        runningSource(5, 5, 0);
        TaskCollector c = collector();
        c.collect(NOW);
        jmx.remove(SRC_TASK).remove(SRC_METRICS);
        assertEquals(List.of(), c.collect(NOW.plusSeconds(60)));
        cluster.connectorStates.put("inventory-cdc", "STOPPED");
        cluster.tasks.remove(SRC);
        assertEquals("connector_stopped", c.collect(NOW.plusSeconds(120)).get(0).exit().reason());
    }

    // A cluster that cannot answer is not a deleted connector.
    @Test
    void anUnreachableClusterIsNotADeletion() {
        runningSource(5, 5, 0);
        TaskCollector c = collector();
        c.collect(NOW);
        jmx.remove(SRC_TASK).remove(SRC_METRICS);
        cluster.unreachable.add("inventory-cdc");
        assertEquals(List.of(), c.collect(NOW.plusSeconds(60)));
        cluster.unreachable.clear();
        cluster.connectorStates.remove("inventory-cdc");
        assertEquals("connector_deleted", c.collect(NOW.plusSeconds(120)).get(0).exit().reason());
    }

    // Lowering tasks.max removes tasks from a running connector. Each one
    // ends, and says so, rather than going silent.
    @Test
    void aRemovedTaskSendsAnExit() {
        runningSource(5, 5, 0);
        TaskCollector c = collector();
        c.collect(NOW);
        jmx.remove(SRC_TASK).remove(SRC_METRICS);
        cluster.tasks.remove(SRC);
        assertEquals("task_removed", c.collect(NOW.plusSeconds(60)).get(0).exit().reason());
    }

    // A task now listed on another worker moved: its new worker reports it.
    @Test
    void aTaskOnAnotherWorkerSendsNoExit() {
        runningSource(5, 5, 0);
        TaskCollector c = collector();
        c.collect(NOW);
        jmx.remove(SRC_TASK).remove(SRC_METRICS);
        cluster.tasks.put(SRC, new TaskHealth("RUNNING", "10.0.3.9:8083", "source"));
        for (int i = 1; i <= 6; i++) {
            assertEquals(List.of(), c.collect(NOW.plusSeconds(60L * i)));
        }
    }

    // Under exactly-once, Connect builds a consumer for every source task
    // with the task's consumer client id. It is not a second start.
    @Test
    void anExactlyOnceSourceCountsOneStartPerStart() {
        AckInterceptor ack = new AckInterceptor();
        ack.configure(Map.of("client.id", "connector-producer-inventory-cdc-0"));
        new io.turbolytics.turbostats.connect.intercept.ConsumeInterceptor()
                .configure(Map.of("client.id", "connector-consumer-inventory-cdc-0"));
        runningSource(5, 5, 0);
        assertEquals(0L, collector().collect(NOW).get(0).pipeline().restartCount());
    }

    // During a rebalance the herder refuses config requests. The last good
    // hash and type stand, rather than a false config change.
    @Test
    void anUnavailableConfigKeepsTheLastGoodHashAndType() {
        runningSource(5, 5, 0);
        TaskCollector c = collector();
        Bundle first = c.collect(NOW).get(0);
        cluster.configUnavailable = true;
        Bundle second = c.collect(NOW.plusSeconds(60)).get(0);
        assertEquals(first.instance().configHash(), second.instance().configHash());
        assertEquals("postgres", second.instance().sourceType());
    }

    // connector-metrics exists only on the worker running the connector. A
    // version seen once stands, so it does not flip on every rebalance.
    @Test
    void theConnectorVersionIsRemembered() {
        runningSource(5, 5, 0);
        jmx.put("kafka.connect:type=connector-metrics,connector=inventory-cdc", "connector-version", "3.0.8.Final");
        TaskCollector c = collector();
        assertEquals("3.0.8.Final", c.collect(NOW).get(0).instance().version());
        jmx.remove("kafka.connect:type=connector-metrics,connector=inventory-cdc");
        assertEquals("3.0.8.Final", c.collect(NOW.plusSeconds(60)).get(0).instance().version());
    }

    // Rows discarded under errors.tolerance=all are silent data loss, and
    // the DLQ's rows are diverted: both are the contract's fields.
    @Test
    void droppedAndDivertedRowsAreReported() {
        runningSource(5, 5, 0);
        String errors = "kafka.connect:type=task-error-metrics,connector=inventory-cdc,task=0";
        jmx.put(errors, "total-records-skipped", 7.0)
                .put(errors, "deadletterqueue-produce-requests", 4.0)
                .put(errors, "deadletterqueue-produce-failures", 1.0);
        Bundle b = collector().collect(NOW).get(0);
        assertEquals(7L, b.pipeline().errorRowsDropped());
        assertEquals(3L, b.pipeline().dlqRows());
    }

    // One task that cannot be read must not cost every other task its
    // report, or the whole worker goes dark.
    @Test
    void oneFailingTaskDoesNotDropTheOthers() {
        runningSource(5, 5, 0);
        jmx.put(SNK_TASK, "status", "running")
                .put(SNK_METRICS, "sink-record-read-total", 1.0)
                .put(SNK_METRICS, "sink-record-send-total", 1.0);
        cluster.tasks.put(SNK, new TaskHealth("RUNNING", "10.0.3.8:8083", "sink"));
        cluster.connectorStates.put("customers-sink", "RUNNING");
        cluster.throwing.add(SNK);
        List<Bundle> out = collector().collect(NOW);
        assertEquals(1, out.size());
        assertEquals("prod-connect/inventory-cdc/0", out.get(0).instance().id());
    }

    // Whatever the collector builds must validate, not just hand-made
    // fixtures: a source bundle, a sink bundle and an exit bundle.
    @Test
    void collectedBundlesValidate() throws Exception {
        runningSource(50, 50, 10);
        jmx.put(SNK_TASK, "status", "running")
                .put(SNK_METRICS, "sink-record-read-total", 1000.0)
                .put(SNK_METRICS, "sink-record-send-total", 1000.0)
                .put(SNK_METRICS, "offset-commit-completion-total", 12.0);
        cluster.tasks.put(SNK, new TaskHealth("RUNNING", "10.0.3.8:8083", "sink"));
        cluster.connectorStates.put("customers-sink", "RUNNING");
        TaskCollector c = collector();
        List<Bundle> out = new ArrayList<>(c.collect(NOW));
        jmx.remove(SNK_TASK).remove(SNK_METRICS);
        cluster.connectorStates.remove("customers-sink");
        out.addAll(c.collect(NOW.plusSeconds(60)));
        // Source and sink, then the source again and the sink's exit.
        assertEquals(4, out.size());
        assertEquals(1, out.stream().filter(b -> b.exit() != null).count());
        for (Bundle b : out) {
            assertEquals(java.util.Set.of(), BundleSchemaTest.validate(b.toJson()), b.toJson());
        }
    }

    // Live against Control, a snapshot finished before the second report,
    // so the input never rose between two reports and the task read as never
    // having worked. Work seen on the first report happened no earlier than
    // the task's start.
    @Test
    void workBeforeTheFirstReportDatesFromTheTaskStart() {
        AckInterceptor ack = new AckInterceptor();
        ack.configure(Map.of("client.id", "connector-producer-inventory-cdc-0"));
        long start = TaskCounters.find(SRC).orElseThrow().snapshot().lastProducerStartMillis();
        runningSource(20000, 20000, 0);
        Bundle b = collector().collect(NOW).get(0);
        assertEquals(Instant.ofEpochMilli(start), b.pipeline().lastMessageAt());
        assertEquals(Instant.ofEpochMilli(start), b.lastActivityAt());
    }

    // Without the interceptor's start time, the worker's start is the
    // earliest the work can have happened.
    @Test
    void withoutATaskStartWorkDatesFromTheWorkerStart() {
        runningSource(20000, 20000, 0);
        Bundle b = collector().collect(NOW).get(0);
        assertEquals(Fixtures.process().startedAt(), b.pipeline().lastMessageAt());
    }

    @Test
    void noWorkYetIsStillAbsent() {
        runningSource(0, 0, 0);
        assertNull(collector().collect(NOW).get(0).pipeline().lastMessageAt());
    }

    // A sink that finished its rows before the second report wrote them; its
    // last write dates from no earlier than the worker's start.
    @Test
    void aSinksWritesBeforeTheFirstReportDateFromTheStart() {
        jmx.put(SNK_TASK, "status", "running")
                .put(SNK_METRICS, "sink-record-read-total", 1000.0)
                .put(SNK_METRICS, "sink-record-send-total", 1000.0)
                .put(SNK_METRICS, "sink-record-active-count", 0.0);
        cluster.tasks.put(SNK, new TaskHealth("RUNNING", "10.0.3.8:8083", "sink"));
        cluster.connectorStates.put("customers-sink", "RUNNING");
        Bundle b = collector().collect(NOW).get(0);
        assertEquals(Fixtures.process().startedAt(), b.pipeline().lastSinkWriteAt());
    }
}
