package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class ConnectMetricsTest {
    static final TaskKey K = new TaskKey("inventory-cdc", 0);

    // A task is local when its task metrics are registered in this JVM:
    // Connect registers them only on the worker that runs the task.
    @Test
    void localTasksAreTheRegisteredTaskMetrics() {
        FakeJmx jmx = new FakeJmx()
                .put("kafka.connect:type=connector-task-metrics,connector=inventory-cdc,task=0", "status", "running")
                .put("kafka.connect:type=connector-task-metrics,connector=customers-sink,task=2", "status", "running")
                .put("kafka.connect:type=connect-worker-metrics", "task-count", 2.0);
        assertEquals(List.of(new TaskKey("customers-sink", 2), K),
                new ConnectMetrics(jmx).localTasks().stream().sorted(
                        (a, b) -> a.connector().compareTo(b.connector())).toList());
    }

    // Review focus: Kafka quotes a value with JMX-special characters, and
    // the reporter must read the connector's real name back.
    @Test
    void aQuotedConnectorNameIsUnquoted() {
        FakeJmx jmx = new FakeJmx()
                .put("kafka.connect:type=connector-task-metrics,connector=\"a:b,c\",task=0", "status", "running")
                .put("kafka.connect:type=source-task-metrics,connector=\"a:b,c\",task=0", "source-record-poll-total", 7.0);
        ConnectMetrics m = new ConnectMetrics(jmx);
        TaskKey k = m.localTasks().get(0);
        assertEquals("a:b,c", k.connector());
        assertEquals(OptionalLong.of(7), m.total("source-task-metrics", k, "source-record-poll-total"));
    }

    // Kafka keeps totals as doubles. Absent and NaN are not zero.
    @Test
    void totalsAreLongsAndMissingIsEmpty() {
        FakeJmx jmx = new FakeJmx()
                .put("kafka.connect:type=source-task-metrics,connector=inventory-cdc,task=0", "source-record-poll-total", 55000.0)
                .put("kafka.connect:type=source-task-metrics,connector=inventory-cdc,task=0", "source-record-active-count", Double.NaN);
        ConnectMetrics m = new ConnectMetrics(jmx);
        assertEquals(OptionalLong.of(55000), m.total("source-task-metrics", K, "source-record-poll-total"));
        assertTrue(m.total("source-task-metrics", K, "source-record-active-count").isEmpty());
        assertTrue(m.total("source-task-metrics", K, "source-record-write-total").isEmpty());
        assertTrue(m.total("sink-task-metrics", K, "sink-record-read-total").isEmpty());
    }

    @Test
    void theConnectorVersionComesFromConnectorMetrics() {
        FakeJmx jmx = new FakeJmx()
                .put("kafka.connect:type=connector-metrics,connector=inventory-cdc", "connector-version", "3.0.8.Final");
        assertEquals(Optional.of("3.0.8.Final"), new ConnectMetrics(jmx).connectorVersion("inventory-cdc"));
        assertTrue(new ConnectMetrics(jmx).connectorVersion("other").isEmpty());
    }
}
