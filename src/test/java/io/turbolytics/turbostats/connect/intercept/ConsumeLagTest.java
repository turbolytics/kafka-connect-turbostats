package io.turbolytics.turbostats.connect.intercept;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.collect.TaskKey;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConsumeLagTest {
    static final TaskKey K = new TaskKey("s", 0);

    @BeforeEach
    void reset() {
        TaskCounters.clear();
    }

    static ConsumerRecord<Object, Object> record(long ts, TimestampType type) {
        return new ConsumerRecord<>("t", 0, 0L, ts, type, -1, -1, "k", "v", new RecordHeaders(), Optional.empty());
    }

    static ConsumeInterceptor sink() {
        ConsumeInterceptor c = new ConsumeInterceptor();
        c.configure(Map.of("client.id", "connector-consumer-s-0"));
        return c;
    }

    // Now less the newest record in the batch, with the timestamp's type.
    @Test
    void eachBatchMeasuresItsNewestRecord() {
        long now = System.currentTimeMillis();
        sink().onConsume(new ConsumerRecords<>(Map.of(new TopicPartition("t", 0), List.of(
                record(now - 9000, TimestampType.CREATE_TIME), record(now - 2000, TimestampType.CREATE_TIME)))));
        TaskCounters.Snapshot s = TaskCounters.find(K).orElseThrow().snapshot();
        assertTrue(s.eventLagMillis() >= 2000 && s.eventLagMillis() < 3000, "lag " + s.eventLagMillis());
        assertEquals(s.eventLagMillis(), s.maxEventLagMillis());
        assertEquals("kafka_create_time", s.timestampBasis());
        assertTrue(s.eventObservedMillis() >= now);
    }

    // Review focus: no timestamp, no lag.
    @Test
    void recordsWithoutTimestampsGiveNoLag() {
        sink().onConsume(new ConsumerRecords<>(Map.of(new TopicPartition("t", 0), List.of(
                record(-1, TimestampType.NO_TIMESTAMP_TYPE)))));
        assertEquals(-1, TaskCounters.find(K).orElseThrow().snapshot().eventLagMillis());
    }

    // A topic that sets message.timestamp.type stamps with the broker's clock.
    @Test
    void logAppendTimeIsItsOwnBasis() {
        sink().onConsume(new ConsumerRecords<>(Map.of(new TopicPartition("t", 0), List.of(
                record(System.currentTimeMillis(), TimestampType.LOG_APPEND_TIME)))));
        assertEquals("kafka_log_append_time", TaskCounters.find(K).orElseThrow().snapshot().timestampBasis());
    }
}
