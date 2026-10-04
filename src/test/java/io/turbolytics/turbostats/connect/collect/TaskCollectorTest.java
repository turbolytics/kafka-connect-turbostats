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

        @Override
        public Optional<TaskHealth> task(TaskKey k) {
            return Optional.ofNullable(tasks.get(k));
        }

        @Override
        public Optional<String> connectorState(String connector) {
            return Optional.ofNullable(connectorStates.get(connector));
        }

        @Override
        public Map<String, String> config(String connector) {
            return Map.of("connector.class", connector.equals("customers-sink")
                    ? "io.debezium.connector.jdbc.JdbcSinkConnector"
                    : "io.debezium.connector.postgresql.PostgresConnector");
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
        return new TaskCollector(cfg, new ConnectMetrics(jmx), cluster, () -> Fixtures.process().withHost(null),
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
        for (int i = 0; i < 40; i++) {
            ack.onAcknowledgement(null, null);
        }
        runningSource(50, 50, 10);

        Bundle b = collector().collect(NOW).get(0);
        assertEquals("prod-connect/inventory-cdc/0", b.instance().id());
        assertEquals("prod-connect/inventory-cdc", b.instance().name());
        assertEquals("postgres", b.instance().sourceType());
        assertEquals("kafka", b.instance().sinkType());
        assertEquals("kafka-connect", b.instance().runtime());
        assertEquals("10.0.3.7:8083", b.process().host());
        assertEquals("running", b.pipeline().state());
        assertEquals(50, b.pipeline().messageCount());
        assertEquals(50, b.pipeline().sinkRowsAccepted());
        assertEquals(40, b.pipeline().sinkRowsWritten());
        assertNull(b.pipeline().sinkFlushCount());
        assertEquals(0, b.pipeline().stateCommitCount());
        assertEquals(1L, b.pipeline().restartCount());
        assertTrue(b.pipeline().lastSinkWriteAt() != null);
    }

    // Without the interceptor's counts, written is records written less
    // records in flight, and there is no acknowledgment time to report.
    @Test
    void withoutTheInterceptorWrittenFallsBackToConnectsCounts() {
        runningSource(50, 50, 10);
        Bundle b = collector().collect(NOW).get(0);
        assertEquals(40, b.pipeline().sinkRowsWritten());
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
        assertEquals(12L, b.pipeline().sinkFlushCount());
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
}
