package io.turbolytics.turbostats.connect.intercept;

import io.turbolytics.turbostats.connect.collect.TaskKey;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Notes each sink task's start and its last batch with records. Plan B
 * adds the newest record timestamp per batch, for event lag. Runs once per
 * poll on the task's thread; no exception may escape.
 */
public final class ConsumeInterceptor implements ConsumerInterceptor<Object, Object> {
    private volatile TaskCounters counters;

    @Override
    public void configure(Map<String, ?> configs) {
        try {
            Object id = configs == null ? null : configs.get("client.id");
            TaskKey.fromClientId(id == null ? null : String.valueOf(id)).ifPresent(k -> {
                TaskCounters c = TaskCounters.of(k);
                c.started(System.currentTimeMillis());
                counters = c;
            });
        } catch (Throwable ignored) {
            // Never fail the task over a monitoring counter.
        }
    }

    @Override
    public ConsumerRecords<Object, Object> onConsume(ConsumerRecords<Object, Object> records) {
        try {
            TaskCounters c = counters;
            if (c != null && records != null && !records.isEmpty()) {
                c.batch(System.currentTimeMillis());
            }
        } catch (Throwable ignored) {
            // Never fail the task over a monitoring counter.
        }
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    }

    @Override
    public void close() {
    }
}
