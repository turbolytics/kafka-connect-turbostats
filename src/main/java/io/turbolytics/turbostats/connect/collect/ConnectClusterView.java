package io.turbolytics.turbostats.connect.collect;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.health.ConnectorHealth;
import org.apache.kafka.connect.health.TaskState;

/**
 * Reads ConnectClusterState, which a REST extension receives. Every call
 * swallows failures: an unknown connector throws, and a race with a
 * rebalance can too.
 */
public final class ConnectClusterView implements ClusterView {
    private final ConnectClusterState state;

    public ConnectClusterView(ConnectClusterState state) {
        this.state = state;
    }

    @Override
    public Optional<TaskHealth> task(TaskKey k) {
        try {
            ConnectorHealth h = state.connectorHealth(k.connector());
            TaskState t = h.tasksState().get(k.task());
            if (t == null) {
                return Optional.empty();
            }
            String type = h.type() == null ? "unknown" : h.type().toString().toLowerCase(Locale.ROOT);
            return Optional.of(new TaskHealth(t.state(), t.workerId(), type));
        } catch (Throwable e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<String> connectorState(String connector) {
        try {
            return Optional.of(state.connectorHealth(connector).connectorState().state());
        } catch (Throwable e) {
            return Optional.empty();
        }
    }

    @Override
    public Map<String, String> config(String connector) {
        try {
            Map<String, String> c = state.connectorConfig(connector);
            return c == null ? Map.of() : c;
        } catch (Throwable e) {
            return Map.of();
        }
    }
}
