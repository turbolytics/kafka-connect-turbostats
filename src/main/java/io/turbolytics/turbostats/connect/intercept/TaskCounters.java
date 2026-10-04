package io.turbolytics.turbostats.connect.intercept;

import io.turbolytics.turbostats.connect.collect.TaskKey;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What the interceptors saw of one task, shared with the reporter.
 *
 * A static registry works because the jar sits on the worker classpath:
 * the extension and both interceptors load from the application
 * classloader, so they see one class and one map. The spike confirmed the
 * loader for all three.
 */
public final class TaskCounters {
    private static final ConcurrentHashMap<TaskKey, TaskCounters> REGISTRY = new ConcurrentHashMap<>();

    private final AtomicLong starts = new AtomicLong();
    private final AtomicLong acked = new AtomicLong();
    private volatile long lastStartMillis;
    private volatile long lastAckMillis;
    private volatile long lastBatchMillis;

    public record Snapshot(long starts, long lastStartMillis, long acked, long lastAckMillis, long lastBatchMillis) {
    }

    public static TaskCounters of(TaskKey k) {
        return REGISTRY.computeIfAbsent(k, x -> new TaskCounters());
    }

    public static Optional<TaskCounters> find(TaskKey k) {
        return Optional.ofNullable(REGISTRY.get(k));
    }

    /** Tests only: the registry outlives a test otherwise. */
    public static void clear() {
        REGISTRY.clear();
    }

    /**
     * A new producer or consumer means the task started. Its counts restart
     * with it, as Connect's own counters do, so they share one epoch.
     */
    void started(long nowMillis) {
        acked.set(0);
        lastAckMillis = 0;
        lastBatchMillis = 0;
        lastStartMillis = nowMillis;
        starts.incrementAndGet();
    }

    void acked(long nowMillis) {
        acked.incrementAndGet();
        lastAckMillis = nowMillis;
    }

    void batch(long nowMillis) {
        lastBatchMillis = nowMillis;
    }

    public Snapshot snapshot() {
        return new Snapshot(starts.get(), lastStartMillis, acked.get(), lastAckMillis, lastBatchMillis);
    }
}
