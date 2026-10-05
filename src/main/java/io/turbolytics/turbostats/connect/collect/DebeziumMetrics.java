package io.turbolytics.turbostats.connect.collect;

import io.turbolytics.turbostats.connect.wire.Backfill;
import io.turbolytics.turbostats.connect.wire.EventLag;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
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
        public static final View UNKNOWN = new View(List.of(), true, List.of(), true);
    }

    private final Jmx jmx;

    public DebeziumMetrics(Jmx jmx) {
        this.jmx = jmx;
    }

    /**
     * Without the task's start (taskStartMillis 0) every metric is stale:
     * nothing tells this run's from the run before it.
     */
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
            boolean stale = taskStartMillis <= 0 || jmx.registeredAt(n) < taskStartMillis;
            Map<String, Object> values = new HashMap<>();
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
        return new Backfill(state, running && !incremental,
                max(v.snapshot(), "SnapshotDurationInSeconds"), "table",
                intOrNull(sum(v.snapshot(), "TotalTableCount")), intOrNull(sum(v.snapshot(), "RemainingTableCount")),
                rows(v.snapshot()));
    }

    /** The worst shard's lag; absent before the first event, which Debezium reports as -1. */
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
        // The reading is the last event's lag and stands still while the
        // source is quiet, so it is dated at that event, not now.
        return new EventLag(behind / 1000.0, worst == null ? null : worst / 1000.0,
                since == null ? now : now.minusMillis(since), "source_commit_time");
    }

    /**
     * Absent while a blocking snapshot runs, because the stream has not
     * opened, and when the metrics are missing, stale or unreadable.
     */
    public static Boolean sourceConnected(View v, Backfill b) {
        if (v.streaming().isEmpty() || v.streamingStale()) {
            return null;
        }
        if (b != null && b.blocksStream()) {
            return null;
        }
        boolean connected = true;
        for (Map<String, Object> m : v.streaming()) {
            // An unreadable attribute measured nothing; false would say the
            // source is lost.
            if (!(m.get("Connected") instanceof Boolean c)) {
                return null;
            }
            connected &= c;
        }
        return connected;
    }

    /** Now less the fewest milliseconds since any current context's last event. */
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

    // Debezium uses -1 for "none yet", so only non-negative values count.
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
