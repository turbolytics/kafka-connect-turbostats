package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;
import java.time.Instant;

/**
 * A sink task's lag in messages, from the broker: the worst partition, the
 * sum, how many partitions they summarize, and when the broker answered.
 */
public record MessageLag(long maxMessages, long totalMessages, int partitions, Instant observedAt) {

    void writeInto(JsonObject o) {
        o.put("lag_max_messages", maxMessages)
                .put("lag_total_messages", totalMessages)
                .put("lag_partitions", (long) partitions)
                .put("lag_observed_at", observedAt);
    }
}
