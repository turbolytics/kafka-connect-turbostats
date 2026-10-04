package io.turbolytics.turbostats.connect.intercept;

import io.turbolytics.turbostats.connect.collect.TaskKey;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Counts each source task's acknowledged records.
 *
 * Connect's source-record-write-total counts when a record is sent, before
 * the broker has it; onAcknowledgement is the only per-record
 * acknowledgment the worker exposes. This runs on the producer's I/O
 * thread for every record: one increment, no allocation, and no exception
 * may escape, because a failing interceptor fails the task.
 */
public final class AckInterceptor implements ProducerInterceptor<Object, Object> {
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
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        try {
            TaskCounters c = counters;
            if (c != null && exception == null) {
                c.acked(System.currentTimeMillis());
            }
        } catch (Throwable ignored) {
            // Never fail the task over a monitoring counter.
        }
    }

    @Override
    public void close() {
    }
}
