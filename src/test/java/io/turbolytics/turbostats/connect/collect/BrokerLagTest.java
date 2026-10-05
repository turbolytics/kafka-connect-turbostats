package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.wire.MessageLag;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.TopicPartition;
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
        boolean timeout;
        int closed;

        public Map<String, Set<TopicPartition>> assignments(String group, Duration t) throws Exception {
            if (timeout) {
                throw new TimeoutException("broker");
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
}
