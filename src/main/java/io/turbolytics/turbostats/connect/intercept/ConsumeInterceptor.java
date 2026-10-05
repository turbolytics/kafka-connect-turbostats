package io.turbolytics.turbostats.connect.intercept;

import io.turbolytics.turbostats.connect.collect.TaskKey;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

/**
 * Notes each sink task's start, its last batch with records, and that
 * batch's event lag: now less its newest record timestamp. Runs once per
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
                c.consumerStarted(System.currentTimeMillis());
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
                long now = System.currentTimeMillis();
                c.batch(now);
                long newest = -1;
                TimestampType type = TimestampType.NO_TIMESTAMP_TYPE;
                for (ConsumerRecord<Object, Object> r : records) {
                    if (r.timestamp() > newest && r.timestampType() != TimestampType.NO_TIMESTAMP_TYPE) {
                        newest = r.timestamp();
                        type = r.timestampType();
                    }
                }
                if (newest >= 0) {
                    c.event(Math.max(0, now - newest), now, type == TimestampType.LOG_APPEND_TIME
                            ? "kafka_log_append_time" : "kafka_create_time");
                }
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
