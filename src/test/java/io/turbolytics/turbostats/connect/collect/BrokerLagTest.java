package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.wire.MessageLag;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.junit.jupiter.api.Test;

class BrokerLagTest {
    static final Instant NOW = Instant.parse("2026-10-05T10:00:00Z");
    static final TopicPartition P0 = new TopicPartition("t", 0);
    static final TopicPartition P1 = new TopicPartition("t", 1);
    static final TopicPartition P2 = new TopicPartition("t", 2);

    static class Fake implements GroupAdmin {
        Map<String, Set<TopicPartition>> members = new HashMap<>();
        Map<TopicPartition, Long> committed = new HashMap<>();
        Map<TopicPartition, Long> ends = new HashMap<>();
        Map<String, Exception> fail = new HashMap<>();
        boolean timeout;
        int closed;

        public Map<String, Set<TopicPartition>> assignments(String group, Duration t) throws Exception {
            if (timeout) {
                throw new TimeoutException("broker");
            }
            if (fail.containsKey(group)) {
                throw fail.get(group);
            }
            return members;
        }

        public Map<TopicPartition, Long> committed(String group, Duration t) {
            return committed;
        }

        public Map<TopicPartition, Long> ends(Set<TopicPartition> tps, Duration t) {
            return ends;
        }

        public void close() {
            closed++;
        }
    }

    // The spike's two-task sink: task 0 held partitions 0 and 1, task 1 held 2.
    @Test
    void eachTaskGetsItsPartitionsLag() {
        Fake f = new Fake();
        f.members.put("connector-consumer-s-0", Set.of(P0, P1));
        f.members.put("connector-consumer-s-1", Set.of(P2));
        f.committed.putAll(Map.of(P0, 90L, P1, 100L, P2, 40L));
        f.ends.putAll(Map.of(P0, 100L, P1, 130L, P2, 40L));
        Map<TaskKey, MessageLag> lags = new BrokerLag(() -> f, Duration.ofSeconds(1)).lags(Map.of("s", "connect-s"), NOW);
        assertEquals(new MessageLag(30, 40, 2, NOW), lags.get(new TaskKey("s", 0)));
        assertEquals(new MessageLag(0, 0, 1, NOW), lags.get(new TaskKey("s", 1)));
    }

    // Review focus: a broker that times out leaves lag absent this tick.
    @Test
    void aTimeoutLeavesLagAbsent() {
        Fake f = new Fake();
        f.timeout = true;
        assertTrue(new BrokerLag(() -> f, Duration.ofSeconds(1)).lags(Map.of("s", "connect-s"), NOW).isEmpty());
    }

    // Review focus: no commit yet means unknown lag, not zero or the topic.
    @Test
    void aPartitionWithoutACommitIsLeftOut() {
        Fake f = new Fake();
        f.members.put("connector-consumer-s-0", Set.of(P0, P1));
        f.committed.put(P0, 5L);
        f.ends.putAll(Map.of(P0, 10L, P1, 1000L));
        assertEquals(new MessageLag(5, 5, 1, NOW),
                new BrokerLag(() -> f, Duration.ofSeconds(1)).lags(Map.of("s", "connect-s"), NOW).get(new TaskKey("s", 0)));
    }

    // A member another client owns, such as a consumer sharing the group, is not a task.
    @Test
    void membersThatAreNotThisConnectorsTasksAreIgnored() {
        Fake f = new Fake();
        f.members.put("someone-else", Set.of(P0));
        f.committed.put(P0, 1L);
        f.ends.put(P0, 9L);
        assertTrue(new BrokerLag(() -> f, Duration.ofSeconds(1)).lags(Map.of("s", "connect-s"), NOW).isEmpty());
    }

    // A broker that timed out for one connector is not asked about the
    // rest this tick: each would wait out its own timeout.
    @Test
    void aTimeoutEndsTheTick() {
        Fake f = new Fake();
        f.timeout = true;
        int[] asked = {0};
        GroupAdmin counting = new GroupAdmin() {
            public Map<String, Set<org.apache.kafka.common.TopicPartition>> assignments(String g, Duration t)
                    throws Exception {
                asked[0]++;
                return f.assignments(g, t);
            }

            public Map<org.apache.kafka.common.TopicPartition, Long> committed(String g, Duration t) {
                return Map.of();
            }

            public Map<org.apache.kafka.common.TopicPartition, Long> ends(Set<org.apache.kafka.common.TopicPartition> p,
                    Duration t) {
                return Map.of();
            }

            public void close() {
            }
        };
        new BrokerLag(() -> counting, Duration.ofSeconds(1)).lags(Map.of("a", "connect-a", "b", "connect-b"), NOW);
        assertEquals(1, asked[0]);
    }

    // Review: a denied group is denied on every tick. Rebuilding the client
    // for it would cost each tick a connect and a close, so the client stays,
    // the other connectors still get lag, and the denial is said once.
    @Test
    void aDeniedGroupKeepsTheClientAndWarnsOnce() {
        Fake f = new Fake();
        f.fail.put("connect-a", new ExecutionException(new GroupAuthorizationException("denied")));
        f.members.put("connector-consumer-b-0", Set.of(P0));
        f.committed.put(P0, 1L);
        f.ends.put(P0, 4L);
        int[] built = {0};
        List<String> warnings = new ArrayList<>();
        BrokerLag lag = new BrokerLag(() -> {
            built[0]++;
            return f;
        }, Duration.ofSeconds(1), warnings::add);
        Map<String, String> groups = new TreeMap<>(Map.of("a", "connect-a", "b", "connect-b"));
        lag.lags(groups, NOW);
        Map<TaskKey, MessageLag> second = lag.lags(groups, NOW);
        assertEquals(new MessageLag(3, 3, 1, NOW), second.get(new TaskKey("b", 0)));
        assertEquals(1, built[0]);
        assertEquals(0, f.closed);
        assertEquals(1, warnings.size());
    }

    // Review: the admin client's own timeout arrives wrapped, and is as slow
    // for the next connector as for this one.
    @Test
    void kafkasTimeoutEndsTheTick() {
        Fake f = new Fake();
        f.fail.put("connect-a", new ExecutionException(new org.apache.kafka.common.errors.TimeoutException("slow")));
        f.fail.put("connect-b", new ExecutionException(new org.apache.kafka.common.errors.TimeoutException("slow")));
        int[] built = {0};
        new BrokerLag(() -> {
            built[0]++;
            return f;
        }, Duration.ofSeconds(1)).lags(Map.of("a", "connect-a", "b", "connect-b"), NOW);
        assertEquals(0, f.closed);
        assertEquals(1, built[0]);
    }

    // Review: a broken client is rebuilt on the next tick, not once per
    // connector on this one.
    @Test
    void aBrokenClientIsRebuiltOncePerTick() {
        Fake f = new Fake();
        f.fail.put("connect-a", new IllegalStateException("broken"));
        f.fail.put("connect-b", new IllegalStateException("broken"));
        int[] built = {0};
        new BrokerLag(() -> {
            built[0]++;
            return f;
        }, Duration.ofSeconds(1)).lags(Map.of("a", "connect-a", "b", "connect-b"), NOW);
        assertEquals(1, built[0]);
        assertEquals(1, f.closed);
    }

    // Review: shutdown interrupts the reporter. The interrupt is kept, and
    // no client is built after it.
    @Test
    void anInterruptEndsTheTickAndIsKept() {
        Fake f = new Fake();
        f.fail.put("connect-a", new InterruptedException());
        f.fail.put("connect-b", new InterruptedException());
        int[] built = {0};
        new BrokerLag(() -> {
            built[0]++;
            return f;
        }, Duration.ofSeconds(1)).lags(Map.of("a", "connect-a", "b", "connect-b"), NOW);
        assertTrue(Thread.interrupted());
        assertEquals(1, built[0]);
    }
}
