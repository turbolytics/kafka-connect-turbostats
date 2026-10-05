package io.turbolytics.turbostats.connect.collect;

import io.turbolytics.turbostats.connect.wire.MessageLag;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.kafka.common.TopicPartition;

/**
 * Each sink task's lag in messages, from the broker. The consumer's own
 * records-lag read 0 on a paused sink the broker said was 2000 behind, so
 * the lag is end offset less committed offset over the partitions the
 * task holds. A failed sink task leaves the group and gets no entry.
 */
public final class BrokerLag {
    private final Supplier<GroupAdmin> factory;
    private final Duration timeout;
    private GroupAdmin admin;

    public BrokerLag(Supplier<GroupAdmin> factory, Duration timeout) {
        this.factory = factory;
        this.timeout = timeout;
    }

    /** Keyed by task; a connector the broker could not answer for is absent this tick. */
    public Map<TaskKey, MessageLag> lags(Map<String, String> groupByConnector, Instant now) {
        Map<TaskKey, MessageLag> out = new HashMap<>();
        for (Map.Entry<String, String> e : groupByConnector.entrySet()) {
            try {
                if (admin == null) {
                    admin = factory.get();
                }
                out.putAll(lags(e.getKey(), e.getValue(), now));
            } catch (Exception ex) {
                // A slow broker is asked again next tick on the same client;
                // anything else may be a broken client, so the next tick
                // builds a fresh one.
                if (!(ex instanceof TimeoutException)) {
                    close();
                }
            }
        }
        return out;
    }

    private Map<TaskKey, MessageLag> lags(String connector, String group, Instant now) throws Exception {
        Map<String, Set<TopicPartition>> members = admin.assignments(group, timeout);
        Map<TaskKey, Set<TopicPartition>> tasks = new HashMap<>();
        Set<TopicPartition> all = new HashSet<>();
        members.forEach((clientId, tps) -> {
            Optional<TaskKey> k = TaskKey.fromClientId(clientId).filter(t -> t.connector().equals(connector));
            if (k.isPresent() && !tps.isEmpty()) {
                tasks.computeIfAbsent(k.get(), x -> new HashSet<>()).addAll(tps);
                all.addAll(tps);
            }
        });
        Map<TaskKey, MessageLag> out = new HashMap<>();
        if (tasks.isEmpty()) {
            return out;
        }
        Map<TopicPartition, Long> committed = admin.committed(group, timeout);
        Map<TopicPartition, Long> ends = admin.ends(all, timeout);
        tasks.forEach((k, tps) -> {
            long max = 0;
            long total = 0;
            int counted = 0;
            for (TopicPartition tp : tps) {
                Long c = committed.get(tp);
                Long end = ends.get(tp);
                // No commit yet: the lag is unknown, neither zero nor the
                // whole partition.
                if (c == null || end == null) {
                    continue;
                }
                long lag = Math.max(0, end - c);
                max = Math.max(max, lag);
                total += lag;
                counted++;
            }
            if (counted > 0) {
                out.put(k, new MessageLag(max, total, counted, now));
            }
        });
        return out;
    }

    public void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (RuntimeException ignored) {
                // Closing is best effort; the next tick builds a new client.
            }
            admin = null;
        }
    }
}
