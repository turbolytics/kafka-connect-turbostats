package io.turbolytics.turbostats.connect.intercept;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.collect.TaskKey;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InterceptorTest {
    static final TaskKey SOURCE = new TaskKey("inventory-cdc", 0);
    static final TaskKey SINK = new TaskKey("customers-sink", 0);

    @BeforeEach
    void reset() {
        TaskCounters.clear();
    }

    static AckInterceptor producerFor(String clientId) {
        AckInterceptor i = new AckInterceptor();
        i.configure(Map.of("client.id", clientId));
        return i;
    }

    // Acknowledged means the broker has the record. A failed send is not a
    // write.
    @Test
    void countsAcknowledgmentsNotFailures() {
        AckInterceptor i = producerFor("connector-producer-inventory-cdc-0");
        i.onAcknowledgement(null, null);
        i.onAcknowledgement(null, null);
        i.onAcknowledgement(null, new RuntimeException("broker gone"));
        TaskCounters.Snapshot s = TaskCounters.find(SOURCE).orElseThrow().snapshot();
        assertEquals(2, s.acked());
        assertTrue(s.lastAckMillis() > 0);
    }

    // Each task start builds a new producer, and configure runs once for it.
    // A restart starts a new epoch: the count restarts as Connect's do.
    @Test
    void eachConfigureIsAStartAndResetsTheEpoch() {
        AckInterceptor first = producerFor("connector-producer-inventory-cdc-0");
        first.onAcknowledgement(null, null);
        AckInterceptor second = producerFor("connector-producer-inventory-cdc-0");
        producerFor("connector-producer-inventory-cdc-0");
        TaskCounters.Snapshot s = TaskCounters.find(SOURCE).orElseThrow().snapshot();
        assertEquals(3, s.producerStarts());
        assertEquals(0, s.acked());
        second.onAcknowledgement(null, null);
        assertEquals(1, TaskCounters.find(SOURCE).orElseThrow().snapshot().acked());
    }

    // Sent is what the task handed the producer, before the broker answers.
    @Test
    void countsSends() {
        AckInterceptor i = producerFor("connector-producer-inventory-cdc-0");
        i.onSend(null);
        i.onSend(null);
        assertEquals(2, TaskCounters.find(SOURCE).orElseThrow().snapshot().sent());
    }

    // The worker's own producers and the DLQ producer pass through untouched.
    @Test
    void otherClientsAreIgnoredWithoutError() {
        AckInterceptor i = producerFor("spike-connect-statuses");
        assertDoesNotThrow(() -> i.onAcknowledgement(null, null));
        assertTrue(TaskCounters.find(SOURCE).isEmpty());
    }

    // An interceptor that throws would fail the task it sits in.
    @Test
    void nothingThrows() {
        AckInterceptor i = new AckInterceptor();
        assertDoesNotThrow(() -> i.configure(null));
        assertDoesNotThrow(() -> i.onAcknowledgement(null, null));
        assertDoesNotThrow(() -> i.onSend(null));
        ConsumeInterceptor c = new ConsumeInterceptor();
        assertDoesNotThrow(() -> c.configure(null));
        assertDoesNotThrow(() -> c.onConsume(null));
    }

    @Test
    void aSinkTaskCountsStartsAndBatchesWithRecords() {
        ConsumeInterceptor c = new ConsumeInterceptor();
        c.configure(Map.of("client.id", "connector-consumer-customers-sink-0"));
        TopicPartition tp = new TopicPartition("t", 0);
        c.onConsume(new ConsumerRecords<>(Map.of()));
        assertEquals(0, TaskCounters.find(SINK).orElseThrow().snapshot().lastBatchMillis());
        c.onConsume(new ConsumerRecords<>(Map.of(tp, List.of(new ConsumerRecord<>("t", 0, 0L, "k", "v")))));
        TaskCounters.Snapshot s = TaskCounters.find(SINK).orElseThrow().snapshot();
        assertEquals(1, s.consumerStarts());
        assertTrue(s.lastBatchMillis() > 0);
    }
}
