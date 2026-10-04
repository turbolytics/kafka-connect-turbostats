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
 * task whose connector was stopped or deleted. Called from the reporter's
 * one thread only, so its memory of the last call needs no locking.
 */
public final class TaskCollector {
    private static final String SOURCE = "source-task-metrics";
    private static final String SINK = "sink-task-metrics";
    private static final String ERRORS = "task-error-metrics";

    private final ReporterConfig config;
    private final ConnectMetrics metrics;
    private final ClusterView cluster;
    private final Supplier<ProcessInfo> process;
    private final String runtimeVersion;
    private final Consumer<String> warn;

    /** The last bundle per task, for exits, and what the last call saw, for change times. */
    private final Map<TaskKey, Bundle> last = new HashMap<>();
    private final Map<TaskKey, Seen> seen = new HashMap<>();
    private final Set<TaskKey> warnedBrokenAcks = new HashSet<>();

    private record Seen(long input, Instant inputChangedAt, long output, Instant outputChangedAt,
            long written, long acked) {
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
        List<Bundle> out = new ArrayList<>();
        Set<TaskKey> local = new HashSet<>(metrics.localTasks());
        ProcessInfo jvm = local.isEmpty() ? null : process.get();
        for (TaskKey k : local) {
            Bundle b = bundle(k, jvm, now);
            if (b != null) {
                out.add(b);
                last.put(k, b);
            }
        }
        for (TaskKey gone : new ArrayList<>(last.keySet())) {
            if (local.contains(gone)) {
                continue;
            }
            Bundle prior = last.remove(gone);
            seen.remove(gone);
            exitFor(gone).ifPresent(e -> out.add(prior.withExit(e, now)));
        }
        return out;
    }

    /**
     * A connector deleted or stopped ends its tasks; a task that moved, or
     * whose worker is rebalancing, reports from its new worker instead and
     * sends nothing from here.
     */
    private Optional<Exit> exitFor(TaskKey k) {
        Optional<String> state = cluster.connectorState(k.connector());
        if (state.isEmpty()) {
            return Optional.of(new Exit("connector_deleted", 0));
        }
        if ("STOPPED".equals(state.get())) {
            return Optional.of(new Exit("connector_stopped", 0));
        }
        return Optional.empty();
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
        OptionalLong accepted = metrics.total(type, k, sink ? "sink-record-send-total" : "source-record-write-total");
        if (input.isEmpty() || accepted.isEmpty()) {
            // Starting or moving: no counts yet, and zeros would claim some.
            return null;
        }
        long active = metrics.total(type, k, sink ? "sink-record-active-count" : "source-record-active-count").orElse(0);
        Optional<TaskCounters.Snapshot> counters = TaskCounters.find(k).map(TaskCounters::snapshot)
                .filter(s -> s.starts() > 0);

        Seen prev = seen.get(k);
        Instant inputChangedAt = changedAt(prev == null ? null : prev.input(), prev == null ? null : prev.inputChangedAt(),
                input.getAsLong(), now);

        long written;
        Instant lastWrite;
        Long flushes = null;
        Instant flushChangedAt = null;
        long flushTotal = 0;
        if (sink) {
            written = Math.max(0, input.getAsLong() - active);
            OptionalLong commits = metrics.total(SINK, k, "offset-commit-completion-total");
            if (commits.isPresent()) {
                flushTotal = commits.getAsLong();
                flushes = flushTotal;
                flushChangedAt = changedAt(prev == null ? null : prev.output(),
                        prev == null ? null : prev.outputChangedAt(), flushTotal, now);
            }
            lastWrite = flushChangedAt;
        } else {
            long fallback = Math.max(0, accepted.getAsLong() - active);
            // Broken once writes rose with nothing in flight and no new
            // acknowledgment, and still broken until acknowledgments move:
            // reverting to a stale count would report writes going backwards.
            boolean acksFlat = counters.isPresent() && prev != null && counters.get().acked() == prev.acked();
            if (!acksFlat) {
                warnedBrokenAcks.remove(k);
            }
            boolean broken = acksFlat
                    && ((accepted.getAsLong() > prev.written() && active == 0) || warnedBrokenAcks.contains(k));
            if (counters.isPresent() && !broken) {
                written = counters.get().acked();
                lastWrite = counters.get().lastAckMillis() > 0 ? Instant.ofEpochMilli(counters.get().lastAckMillis()) : null;
            } else {
                if (broken && warnedBrokenAcks.add(k)) {
                    warn.accept("turbostats: acknowledgments for " + k.connector() + "/" + k.task()
                            + " stopped while writes continued; reporting Connect's written count instead");
                }
                written = fallback;
                lastWrite = null;
            }
        }
        seen.put(k, new Seen(input.getAsLong(), inputChangedAt, flushTotal, flushChangedAt,
                accepted.getAsLong(), counters.map(TaskCounters.Snapshot::acked).orElse(0L)));

        Instant lastMessage = inputChangedAt;
        if (sink && counters.isPresent() && counters.get().lastBatchMillis() > 0) {
            lastMessage = Instant.ofEpochMilli(counters.get().lastBatchMillis());
        }

        long errors = metrics.total(ERRORS, k, "total-record-errors").orElse(0);
        OptionalLong lastErrorMillis = metrics.total(ERRORS, k, "last-error-timestamp");
        Instant lastError = lastErrorMillis.isPresent() && lastErrorMillis.getAsLong() > 0
                ? Instant.ofEpochMilli(lastErrorMillis.getAsLong())
                : null;

        Map<String, String> connectorConfig = cluster.config(k.connector());
        String typeOfConnector = Identity.typeOf(connectorConfig.get("connector.class"));
        Instance instance = new Instance(
                Identity.instanceId(config.cluster(), k),
                Identity.instanceName(config.cluster(), k.connector()),
                metrics.connectorVersion(k.connector()).orElse(""),
                "",
                Identity.arch(System.getProperty("os.name"), System.getProperty("os.arch")),
                Identity.configHash(connectorConfig),
                sink ? "kafka" : typeOfConnector,
                sink ? typeOfConnector : "kafka",
                "kafka-connect",
                runtimeVersion,
                ReporterVersion.get(),
                config.labels());

        Pipeline pipeline = new Pipeline(
                state(h.state()),
                counters.map(s -> Instant.ofEpochMilli(s.lastStartMillis())).orElse(null),
                counters.map(s -> s.starts() - 1).orElse(null),
                input.getAsLong(),
                input.getAsLong(),
                errors,
                flushes,
                accepted.getAsLong(),
                written,
                0,
                lastMessage,
                lastWrite,
                lastError);

        return new Bundle(now, config.intervalSeconds(), lastMessage, instance, jvm.withHost(h.workerId()), pipeline,
                null);
    }

    /** When a counter last rose, as this reporter saw it: accurate to one interval. */
    private static Instant changedAt(Long before, Instant beforeAt, long current, Instant now) {
        if (before == null) {
            return null;
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
