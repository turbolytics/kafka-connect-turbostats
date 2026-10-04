package io.turbolytics.turbostats.connect.collect;

import io.turbolytics.turbostats.connect.ReporterVersion;
import io.turbolytics.turbostats.connect.config.ReporterConfig;
import io.turbolytics.turbostats.connect.intercept.TaskCounters;
import io.turbolytics.turbostats.connect.wire.Bundle;
import io.turbolytics.turbostats.connect.wire.Exit;
import io.turbolytics.turbostats.connect.wire.Instance;
import io.turbolytics.turbostats.connect.wire.Pipeline;
import io.turbolytics.turbostats.connect.wire.ProcessInfo;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Builds one bundle per task this worker runs, and the exit bundle for a
 * task whose connector was stopped or deleted, or that was removed. Called
 * from the reporter's one thread only, so its memory of earlier calls needs
 * no locking.
 */
public final class TaskCollector {
    private static final String SOURCE = "source-task-metrics";
    private static final String SINK = "sink-task-metrics";
    private static final String ERRORS = "task-error-metrics";

    /** How many ticks a vanished task waits for a definite answer before it is let go. */
    static final int EXIT_PATIENCE = 5;

    /** A connector's config is fetched through the herder; it is refreshed this often, in ticks. */
    static final int CONFIG_REFRESH_TICKS = 10;

    private final ReporterConfig config;
    private final ConnectMetrics metrics;
    private final ClusterView cluster;
    private final Supplier<ProcessInfo> process;
    private final String runtimeVersion;
    private final Consumer<String> warn;

    private final Map<TaskKey, Bundle> last = new HashMap<>();
    private final Map<TaskKey, Seen> seen = new HashMap<>();
    private final Map<TaskKey, Pending> pending = new HashMap<>();
    private final Map<String, ConnectorFacts> facts = new HashMap<>();
    private final Map<String, String> versions = new HashMap<>();
    private final Set<TaskKey> warnedBrokenAcks = new HashSet<>();
    private final Set<TaskKey> warnedUnreadable = new HashSet<>();
    private long tick;

    private record Seen(long input, Instant inputChangedAt, long written, Instant writtenChangedAt,
            long connectWritten, long acked) {
    }

    private record Pending(Bundle last, int ticks) {
    }

    /** What a connector's config says, kept so a failed fetch never reads as a change. */
    private record ConnectorFacts(String configHash, String type, long fetchedAt) {
    }

    public TaskCollector(ReporterConfig config, ConnectMetrics metrics, ClusterView cluster,
            Supplier<ProcessInfo> process, String runtimeVersion, Consumer<String> warn) {
        this.config = config;
        this.metrics = metrics;
        this.cluster = cluster;
        this.process = process;
        this.runtimeVersion = runtimeVersion;
        this.warn = warn;
    }

    public List<Bundle> collect(Instant now) {
        tick++;
        List<Bundle> out = new ArrayList<>();
        Set<TaskKey> local = new HashSet<>(metrics.localTasks());
        ProcessInfo jvm = local.isEmpty() ? null : process.get();
        for (TaskKey k : local) {
            pending.remove(k);
            try {
                Bundle b = bundle(k, jvm, now);
                if (b != null) {
                    out.add(b);
                    last.put(k, b);
                }
            } catch (RuntimeException e) {
                // One task that cannot be read must not cost the others
                // their reports.
                if (warnedUnreadable.add(k)) {
                    warn.accept("turbostats: could not read " + k.connector() + "/" + k.task() + ": "
                            + e.getClass().getSimpleName());
                }
            }
        }
        for (TaskKey gone : new ArrayList<>(last.keySet())) {
            if (!local.contains(gone)) {
                pending.put(gone, new Pending(last.remove(gone), 0));
                seen.remove(gone);
                // A task that comes back here starts counting afresh: its
                // return is not a restart.
                TaskCounters.forget(gone);
            }
        }
        for (Map.Entry<TaskKey, Pending> e : new ArrayList<>(pending.entrySet())) {
            Decision d = decide(e.getKey(), e.getValue().last());
            if (d.exit() != null) {
                out.add(e.getValue().last().withExit(d.exit(), now));
                pending.remove(e.getKey());
            } else if (d.settled() || e.getValue().ticks() + 1 >= EXIT_PATIENCE) {
                pending.remove(e.getKey());
            } else {
                pending.put(e.getKey(), new Pending(e.getValue().last(), e.getValue().ticks() + 1));
            }
        }
        return out;
    }

    private record Decision(Exit exit, boolean settled) {
        static final Decision WAIT = new Decision(null, false);
        static final Decision MOVED = new Decision(null, true);
    }

    /**
     * Why a task left this worker. Deleted, stopped and removed tasks end, and
     * say so. A task listed on another worker moved, and its new worker
     * reports it. Anything else, a status store that has not caught up or a
     * cluster that cannot answer, waits for a definite answer, up to
     * EXIT_PATIENCE ticks.
     */
    private Decision decide(TaskKey k, Bundle prior) {
        ConnectorLookup c = cluster.connector(k.connector());
        switch (c.answer()) {
            case NOT_FOUND:
                return new Decision(new Exit("connector_deleted", 0), true);
            case UNKNOWN:
                return Decision.WAIT;
            default:
                break;
        }
        if ("STOPPED".equals(c.state())) {
            return new Decision(new Exit("connector_stopped", 0), true);
        }
        Optional<TaskHealth> t;
        try {
            t = cluster.task(k);
        } catch (RuntimeException e) {
            return Decision.WAIT;
        }
        if (t.isEmpty()) {
            return new Decision(new Exit("task_removed", 0), true);
        }
        String host = prior.process() == null ? null : prior.process().host();
        if (t.get().workerId() != null && !t.get().workerId().equals(host)) {
            return Decision.MOVED;
        }
        return Decision.WAIT;
    }

    private Bundle bundle(TaskKey k, ProcessInfo jvm, Instant now) {
        Optional<TaskHealth> health = cluster.task(k);
        if (health.isEmpty() || "UNASSIGNED".equals(health.get().state())) {
            return null;
        }
        TaskHealth h = health.get();
        boolean sink = "sink".equals(h.connectorType());
        String type = sink ? SINK : SOURCE;

        OptionalLong input = metrics.total(type, k, sink ? "sink-record-read-total" : "source-record-poll-total");
        OptionalLong connectOut = metrics.total(type, k, sink ? "sink-record-send-total" : "source-record-write-total");
        if (input.isEmpty() || connectOut.isEmpty()) {
            // Starting or moving: no counts yet, and zeros would claim some.
            return null;
        }
        long active = metrics.total(type, k, sink ? "sink-record-active-count" : "source-record-active-count").orElse(0);
        Optional<TaskCounters.Snapshot> counters = TaskCounters.find(k).map(TaskCounters::snapshot)
                .filter(s -> (sink ? s.consumerStarts() : s.producerStarts()) > 0);
        Seen prev = seen.get(k);
        long starts = counters.map(c -> sink ? c.consumerStarts() : c.producerStarts()).orElse(0L);
        long startMillis = counters.map(c -> sink ? c.lastConsumerStartMillis() : c.lastProducerStartMillis()).orElse(0L);
        // Work already counted on the first report happened no earlier than
        // the task's start, or the worker's when the task's is unknown. A
        // counter compared only between reports otherwise never dates work
        // done before the second report, and a snapshot that finished in the
        // first interval read as a task that never worked.
        Instant earliest = starts > 0 ? Instant.ofEpochMilli(startMillis) : jvm.startedAt();

        long accepted;
        long written;
        Instant lastWrite;
        if (sink) {
            accepted = connectOut.getAsLong();
            // Records read less records in flight is records Connect has
            // finished. Its commit counter is not used: Connect raises it
            // even when it skips a commit because nothing changed, as it does
            // every flush interval while the destination is down.
            written = Math.max(0, input.getAsLong() - active);
            lastWrite = changedAt(prev == null ? null : prev.written(), prev == null ? null : prev.writtenChangedAt(),
                    written, now, earliest);
        } else {
            // source-record-write-total counts acknowledged batches (Connect
            // records it in the producer callback), so it is the fallback for
            // written, as is: subtracting records in flight would count them
            // twice.
            long connectWritten = connectOut.getAsLong();
            boolean acksFlat = counters.isPresent() && prev != null && counters.get().acked() == prev.acked();
            if (!acksFlat) {
                warnedBrokenAcks.remove(k);
            }
            // Broken once Connect's written count rose with nothing in flight
            // and no new acknowledgment, and still broken until
            // acknowledgments move: reverting to a stale count would report
            // writes going backwards.
            boolean broken = acksFlat
                    && ((connectWritten > prev.connectWritten() && active == 0) || warnedBrokenAcks.contains(k));
            if (counters.isPresent() && !broken) {
                accepted = counters.get().sent();
                written = counters.get().acked();
                lastWrite = counters.get().lastAckMillis() > 0 ? Instant.ofEpochMilli(counters.get().lastAckMillis()) : null;
            } else {
                if (broken && warnedBrokenAcks.add(k)) {
                    warn.accept("turbostats: acknowledgments for " + k.connector() + "/" + k.task()
                            + " stopped while writes continued; reporting Connect's written count instead");
                }
                accepted = connectWritten;
                written = connectWritten;
                lastWrite = null;
            }
        }

        Instant inputChangedAt = changedAt(prev == null ? null : prev.input(), prev == null ? null : prev.inputChangedAt(),
                input.getAsLong(), now, earliest);
        seen.put(k, new Seen(input.getAsLong(), inputChangedAt, written, lastWrite == null && sink ? null : lastWrite,
                sink ? 0 : connectOut.getAsLong(), counters.map(TaskCounters.Snapshot::acked).orElse(0L)));

        Instant lastMessage = inputChangedAt;
        if (sink && counters.isPresent() && counters.get().lastBatchMillis() > 0) {
            lastMessage = Instant.ofEpochMilli(counters.get().lastBatchMillis());
        }

        long errors = metrics.total(ERRORS, k, "total-record-errors").orElse(0);
        OptionalLong lastErrorMillis = metrics.total(ERRORS, k, "last-error-timestamp");
        Instant lastError = lastErrorMillis.isPresent() && lastErrorMillis.getAsLong() > 0
                ? Instant.ofEpochMilli(lastErrorMillis.getAsLong())
                : null;
        OptionalLong skipped = metrics.total(ERRORS, k, "total-records-skipped");
        OptionalLong dlqRequests = metrics.total(ERRORS, k, "deadletterqueue-produce-requests");
        Long dlq = dlqRequests.isPresent()
                ? Math.max(0, dlqRequests.getAsLong() - metrics.total(ERRORS, k, "deadletterqueue-produce-failures").orElse(0))
                : null;

        ConnectorFacts f = facts(k.connector());
        String version = metrics.connectorVersion(k.connector()).orElse(null);
        if (version != null && !version.isEmpty()) {
            versions.put(k.connector(), version);
        }
        Instance instance = new Instance(
                Identity.instanceId(config.cluster(), k),
                Identity.instanceName(config.cluster(), k.connector()),
                versions.getOrDefault(k.connector(), ""),
                "",
                Identity.arch(System.getProperty("os.name"), System.getProperty("os.arch")),
                f == null ? "" : f.configHash(),
                sink ? "kafka" : f == null ? null : f.type(),
                sink ? f == null ? null : f.type() : "kafka",
                "kafka-connect",
                runtimeVersion,
                ReporterVersion.get(),
                config.labels());

        Pipeline pipeline = new Pipeline(
                state(h.state()),
                starts > 0 ? Instant.ofEpochMilli(startMillis) : null,
                starts > 0 ? starts - 1 : null,
                input.getAsLong(),
                input.getAsLong(),
                errors,
                null,
                accepted,
                written,
                0,
                lastMessage,
                lastWrite,
                lastError,
                skipped.isPresent() ? skipped.getAsLong() : null,
                dlq);

        return new Bundle(now, config.intervalSeconds(), lastMessage, instance, jvm.withHost(h.workerId()), pipeline,
                null);
    }

    /**
     * A connector's config hash and type, fetched once per connector rather
     * than per task, refreshed every CONFIG_REFRESH_TICKS, and kept when a
     * fetch fails: the herder refuses requests during a rebalance, and an
     * empty config would read as a config change.
     */
    private ConnectorFacts facts(String connector) {
        ConnectorFacts f = facts.get(connector);
        if (f != null && tick - f.fetchedAt() < CONFIG_REFRESH_TICKS) {
            return f;
        }
        Map<String, String> c = cluster.config(connector);
        if (c.isEmpty()) {
            return f;
        }
        ConnectorFacts fresh = new ConnectorFacts(Identity.configHash(c, config.credential().configHashKey()),
                Identity.typeOf(c.get("connector.class")), tick);
        facts.put(connector, fresh);
        return fresh;
    }

    /**
     * When a counter last rose, as this reporter saw it: accurate to one
     * interval. On the first report a non-zero counter rose at some point
     * since earliest, and earliest is the conservative answer: it can make a
     * task look idle sooner, never fresher than it is.
     */
    private static Instant changedAt(Long before, Instant beforeAt, long current, Instant now, Instant earliest) {
        if (before == null) {
            return current > 0 ? earliest : null;
        }
        return current > before ? now : beforeAt;
    }

    /** A value this map does not know reads as unknown, never as running. */
    static String state(String connectState) {
        if (connectState == null) {
            return "unknown";
        }
        return switch (connectState) {
            case "RUNNING" -> "running";
            case "PAUSED" -> "paused";
            case "FAILED" -> "failed";
            case "RESTARTING" -> "starting";
            case "STOPPED" -> "stopped";
            default -> "unknown";
        };
    }
}
