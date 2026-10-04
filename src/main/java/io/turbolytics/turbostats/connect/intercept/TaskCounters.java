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
 *
 * Producer and consumer starts are counted apart. Under exactly-once,
 * Connect builds a consumer for every source task under the task's consumer
 * client id, beside its producer; counting both would make each start two.
 * A source task's starts are its producer's, a sink task's its consumer's.
 */
public final class TaskCounters {
    private static final ConcurrentHashMap<TaskKey, TaskCounters> REGISTRY = new ConcurrentHashMap<>();

    private final AtomicLong producerStarts = new AtomicLong();
    private final AtomicLong consumerStarts = new AtomicLong();
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong acked = new AtomicLong();
    private volatile long lastProducerStartMillis;
    private volatile long lastConsumerStartMillis;
    private volatile long lastAckMillis;
    private volatile long lastBatchMillis;

    public record Snapshot(
            long producerStarts,
            long lastProducerStartMillis,
            long consumerStarts,
            long lastConsumerStartMillis,
            long sent,
            long acked,
            long lastAckMillis,
            long lastBatchMillis) {
    }

    public static TaskCounters of(TaskKey k) {
        return REGISTRY.computeIfAbsent(k, x -> new TaskCounters());
    }

    public static Optional<TaskCounters> find(TaskKey k) {
        return Optional.ofNullable(REGISTRY.get(k));
    }

    /**
     * Forgets a task that left this worker. A task that comes back, after a
     * rebalance or a move, starts counting afresh rather than counting its
     * return as a restart, and a deleted connector's entries do not pile up.
     */
    public static void forget(TaskKey k) {
        REGISTRY.remove(k);
    }

    /** Tests only: the registry outlives a test otherwise. */
    public static void clear() {
        REGISTRY.clear();
    }

    /**
     * A new producer means the source task started. Its counts restart with
     * it, as Connect's own counters do, so they share one epoch.
     */
    void producerStarted(long nowMillis) {
        sent.set(0);
        acked.set(0);
        lastAckMillis = 0;
        lastProducerStartMillis = nowMillis;
        producerStarts.incrementAndGet();
    }

    void consumerStarted(long nowMillis) {
        lastBatchMillis = 0;
        lastConsumerStartMillis = nowMillis;
        consumerStarts.incrementAndGet();
    }

    void sent() {
        sent.incrementAndGet();
    }

    void acked(long nowMillis) {
        acked.incrementAndGet();
        lastAckMillis = nowMillis;
    }

    void batch(long nowMillis) {
        lastBatchMillis = nowMillis;
    }

    public Snapshot snapshot() {
        return new Snapshot(producerStarts.get(), lastProducerStartMillis, consumerStarts.get(),
                lastConsumerStartMillis, sent.get(), acked.get(), lastAckMillis, lastBatchMillis);
    }
}
