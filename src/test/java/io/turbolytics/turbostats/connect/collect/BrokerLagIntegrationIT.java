package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.turbolytics.turbostats.connect.wire.MessageLag;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Broker lag through a real admin client: a fake GroupAdmin proves the
 * arithmetic, only a broker proves that members, commits and end offsets
 * come back keyed as the arithmetic expects.
 */
class BrokerLagIntegrationIT {
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

    static KafkaConsumer<String, String> consumer(String clientId) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "connect-it");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer());
    }

    // Two tasks holding a partition each, both committed at 4 of 10.
    @Test
    void eachTaskGetsTheLagOfItsPartition() throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("t", 2, (short) 1))).all().get();
        }
        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (KafkaProducer<String, String> p = new KafkaProducer<>(pp, new StringSerializer(), new StringSerializer())) {
            for (int part = 0; part < 2; part++) {
                for (int i = 0; i < 10; i++) {
                    p.send(new ProducerRecord<>("t", part, "k", "v")).get();
                }
            }
        }
        try (KafkaConsumer<String, String> c0 = consumer("connector-consumer-it-0");
                KafkaConsumer<String, String> c1 = consumer("connector-consumer-it-1")) {
            c0.subscribe(List.of("t"));
            c1.subscribe(List.of("t"));
            Instant deadline = Instant.now().plusSeconds(60);
            while (c0.assignment().size() != 1 || c1.assignment().size() != 1) {
                if (Instant.now().isAfter(deadline)) {
                    throw new AssertionError("group never settled: " + c0.assignment() + " " + c1.assignment());
                }
                c0.poll(Duration.ofMillis(200));
                c1.poll(Duration.ofMillis(200));
            }
            for (KafkaConsumer<String, String> c : List.of(c0, c1)) {
                TopicPartition tp = c.assignment().iterator().next();
                c.commitSync(Map.of(tp, new OffsetAndMetadata(4)));
            }

            BrokerLag lag = new BrokerLag(
                    () -> new KafkaGroupAdmin(AdminSettings.from(Map.of("bootstrap.servers", kafka.getBootstrapServers()))),
                    Duration.ofSeconds(10));
            try {
                Map<TaskKey, MessageLag> lags = lag.lags(Map.of("it", "connect-it"), Instant.now());
                assertEquals(6, lags.get(new TaskKey("it", 0)).totalMessages());
                assertEquals(6, lags.get(new TaskKey("it", 1)).totalMessages());
                assertEquals(1, lags.get(new TaskKey("it", 0)).partitions());
            } finally {
                lag.close();
            }
        }
    }
}
