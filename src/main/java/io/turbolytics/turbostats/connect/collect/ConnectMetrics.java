package io.turbolytics.turbostats.connect.collect;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import javax.management.ObjectName;

/** Connect's own metrics for the tasks this worker runs. */
public final class ConnectMetrics {
    private static final String DOMAIN = "kafka.connect";
    private final Jmx jmx;

    public ConnectMetrics(Jmx jmx) {
        this.jmx = jmx;
    }

    /** Connect registers a task's metrics only on the worker that runs it. */
    public List<TaskKey> localTasks() {
        List<TaskKey> out = new ArrayList<>();
        for (ObjectName n : jmx.query(DOMAIN + ":type=connector-task-metrics,*")) {
            String connector = unquote(n.getKeyProperty("connector"));
            String task = unquote(n.getKeyProperty("task"));
            if (connector == null || task == null) {
                continue;
            }
            try {
                out.add(new TaskKey(connector, Integer.parseInt(task)));
            } catch (NumberFormatException ignored) {
                // Not a task metrics name this reporter understands.
            }
        }
        return out;
    }

    /** A cumulative metric as a long; empty when absent or NaN, never a false zero. */
    public OptionalLong total(String type, TaskKey k, String attribute) {
        ObjectName n = find(type, k.connector(), Integer.toString(k.task()));
        if (n == null) {
            return OptionalLong.empty();
        }
        Object v = jmx.attribute(n, attribute);
        if (!(v instanceof Number num) || Double.isNaN(num.doubleValue())) {
            return OptionalLong.empty();
        }
        return OptionalLong.of((long) num.doubleValue());
    }

    /** Registered on the worker that runs the connector itself, which may be another. */
    public Optional<String> connectorVersion(String connector) {
        ObjectName n = find("connector-metrics", connector, null);
        Object v = n == null ? null : jmx.attribute(n, "connector-version");
        return v == null ? Optional.empty() : Optional.of(String.valueOf(v));
    }

    /**
     * Matches on unquoted key values rather than building a name, because
     * Kafka quotes a value that holds a JMX-special character.
     */
    private ObjectName find(String type, String connector, String task) {
        for (ObjectName n : jmx.query(DOMAIN + ":type=" + type + ",*")) {
            if (connector.equals(unquote(n.getKeyProperty("connector")))
                    && (task == null || task.equals(unquote(n.getKeyProperty("task"))))) {
                return n;
            }
        }
        return null;
    }

    static String unquote(String v) {
        if (v != null && v.length() >= 2 && v.startsWith("\"") && v.endsWith("\"")) {
            try {
                return ObjectName.unquote(v);
            } catch (IllegalArgumentException e) {
                return v;
            }
        }
        return v;
    }
}
