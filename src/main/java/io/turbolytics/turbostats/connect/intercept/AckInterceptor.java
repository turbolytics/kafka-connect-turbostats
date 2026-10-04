package io.turbolytics.turbostats.connect.intercept;

import io.turbolytics.turbostats.connect.collect.TaskKey;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Counts each source task's sends and acknowledgments, and when it last
 * had one acknowledged.
 *
 * Sends are what the task handed the producer; acknowledgments are what
 * the broker has. Connect's source-record-write-total counts per
 * acknowledged batch, so only this sees records sent and not yet
 * acknowledged, and the exact time of the last acknowledgment. Runs on the
 * task's and the producer's threads for every record: one increment, no
 * allocation, and no exception may escape, because a failing interceptor
 * fails the task.
 */
public final class AckInterceptor implements ProducerInterceptor<Object, Object> {
    private volatile TaskCounters counters;

    @Override
    public void configure(Map<String, ?> configs) {
        try {
            Object id = configs == null ? null : configs.get("client.id");
            TaskKey.fromClientId(id == null ? null : String.valueOf(id)).ifPresent(k -> {
                TaskCounters c = TaskCounters.of(k);
                c.producerStarted(System.currentTimeMillis());
                counters = c;
            });
        } catch (Throwable ignored) {
            // Never fail the task over a monitoring counter.
        }
    }

    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {
        try {
            TaskCounters c = counters;
            if (c != null) {
                c.sent();
            }
        } catch (Throwable ignored) {
            // Never fail the task over a monitoring counter.
        }
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
