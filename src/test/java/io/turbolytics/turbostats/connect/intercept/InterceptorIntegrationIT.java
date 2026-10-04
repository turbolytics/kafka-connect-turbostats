package io.turbolytics.turbostats.connect.intercept;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.collect.TaskKey;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The interceptors inside real Kafka clients against a real broker. A unit
 * test calls onAcknowledgement by hand; only a broker proves the client
 * calls it once per acknowledged record, and calls configure once per
 * client.
 */
class InterceptorIntegrationIT {
    static KafkaContainer kafka;

    @BeforeAll
    static void start() {
        kafka = new KafkaContainer("apache/kafka:3.8.0");
        kafka.start();
    }

    @AfterAll
    static void stop() {
        if (kafka != null) {
            kafka.stop();
        }
    }

    @BeforeEach
    void reset() {
        TaskCounters.clear();
    }

    static KafkaProducer<String, String> producer(String clientId) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, AckInterceptor.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(p, new StringSerializer(), new StringSerializer());
    }

    // Every record the broker acknowledged, and no more.
    @Test
    void countsWhatTheBrokerAcknowledged() {
        try (KafkaProducer<String, String> p = producer("connector-producer-it-cdc-0")) {
            for (int i = 0; i < 500; i++) {
                p.send(new ProducerRecord<>("it-acks", "k" + i, "v" + i));
            }
            p.flush();
        }
        TaskCounters.Snapshot s = TaskCounters.find(new TaskKey("it-cdc", 0)).orElseThrow().snapshot();
        assertEquals(1, s.starts());
        assertEquals(500, s.acked());
        assertTrue(s.lastAckMillis() > 0);
    }

    // A second producer for the same task is a restart: one more start, and
    // the acknowledged count begins again with the new epoch.
    @Test
    void aNewProducerIsOneStart() {
        try (KafkaProducer<String, String> p = producer("connector-producer-it-restart-0")) {
            p.send(new ProducerRecord<>("it-acks", "k", "v"));
            p.flush();
        }
        try (KafkaProducer<String, String> p = producer("connector-producer-it-restart-0")) {
            p.flush();
        }
        TaskCounters.Snapshot s = TaskCounters.find(new TaskKey("it-restart", 0)).orElseThrow().snapshot();
        assertEquals(2, s.starts());
        assertEquals(0, s.acked());
    }

    @Test
    void aConsumerNotesItsBatches() {
        try (KafkaProducer<String, String> p = producer("not-a-task")) {
            p.send(new ProducerRecord<>("it-consume", "k", "v"));
            p.flush();
        }
        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        c.put(ConsumerConfig.CLIENT_ID_CONFIG, "connector-consumer-it-sink-0");
        c.put(ConsumerConfig.GROUP_ID_CONFIG, "connect-it-sink");
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumeInterceptor.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(c, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of("it-consume"));
            Instant deadline = Instant.now().plusSeconds(30);
            while (Instant.now().isBefore(deadline) && consumer.poll(Duration.ofMillis(500)).isEmpty()) {
                // Polls until the group joins and the record arrives.
            }
        }
        TaskCounters.Snapshot s = TaskCounters.find(new TaskKey("it-sink", 0)).orElseThrow().snapshot();
        assertEquals(1, s.starts());
        assertTrue(s.lastBatchMillis() > 0);
    }
}
