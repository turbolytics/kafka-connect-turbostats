package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * ConnectMetrics against Kafka's own JmxReporter, the reporter Connect
 * registers its metrics through. The fake in the unit tests was written by
 * hand; this proves the real MBean names, quoting and value types read the
 * same way.
 */
class ConnectMetricsIntegrationIT {
    Metrics metrics;

    @BeforeEach
    void start() {
        metrics = new Metrics(new MetricConfig(), List.of(new JmxReporter()), Time.SYSTEM,
                new KafkaMetricsContext("kafka.connect"));
    }

    @AfterEach
    void stop() {
        metrics.close();
    }

    void task(String connector, int task, double polled) {
        Map<String, String> tags = Map.of("connector", connector, "task", Integer.toString(task));
        metrics.addMetric(metrics.metricName("status", "connector-task-metrics", "", tags),
                (Gauge<String>) (config, now) -> "running");
        MetricName poll = metrics.metricName("source-record-poll-total", "source-task-metrics", "", tags);
        Sensor s = metrics.sensor(connector + "-" + task + "-poll");
        s.add(poll, new CumulativeSum());
        s.record(polled);
    }

    @Test
    void readsTasksAndTotalsAsConnectRegistersThem() {
        task("orders-cdc-v2", 0, 42);
        ConnectMetrics m = new ConnectMetrics(new PlatformJmx());
        TaskKey k = new TaskKey("orders-cdc-v2", 0);
        assertTrue(m.localTasks().contains(k));
        assertEquals(OptionalLong.of(42), m.total("source-task-metrics", k, "source-record-poll-total"));
    }

    // Kafka's reporter quotes a tag value that holds a JMX-special
    // character. The reporter must find the task under its real name.
    @Test
    void aConnectorNameKafkaQuotesIsFound() {
        task("a:b", 1, 7);
        ConnectMetrics m = new ConnectMetrics(new PlatformJmx());
        TaskKey k = new TaskKey("a:b", 1);
        assertTrue(m.localTasks().contains(k), m.localTasks().toString());
        assertEquals(OptionalLong.of(7), m.total("source-task-metrics", k, "source-record-poll-total"));
    }
}
