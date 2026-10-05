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
