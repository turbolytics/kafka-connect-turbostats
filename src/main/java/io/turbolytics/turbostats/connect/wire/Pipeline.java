package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;
import java.time.Instant;

/**
 * The pipeline section for one task. The six primitive counters are
 * required by v1. sinkFlushCount is null for a source task, which never
 * flushes in batches.
 */
public record Pipeline(
        String state,
        Instant startedAt,
        Long restartCount,
        long messageCount,
        long handlerRowsRead,
        long errorCount,
        Long sinkFlushCount,
        long sinkRowsAccepted,
        long sinkRowsWritten,
        long stateCommitCount,
        Instant lastMessageAt,
        Instant lastSinkWriteAt,
        Instant lastErrorAt) {

    public Pipeline withState(String newState) {
        return new Pipeline(newState, startedAt, restartCount, messageCount, handlerRowsRead, errorCount,
                sinkFlushCount, sinkRowsAccepted, sinkRowsWritten, stateCommitCount, lastMessageAt,
                lastSinkWriteAt, lastErrorAt);
    }

    public JsonObject toJson() {
        return new JsonObject()
                .put("state", state)
                .put("started_at", startedAt)
                .put("restart_count", restartCount)
                .put("message_count", messageCount)
                .put("handler_rows_read", handlerRowsRead)
                .put("error_count", errorCount)
                .put("sink_flush_count", sinkFlushCount)
                .put("sink_rows_accepted", sinkRowsAccepted)
                .put("sink_rows_written", sinkRowsWritten)
                .put("state_commit_count", stateCommitCount)
                .put("last_message_at", lastMessageAt)
                .put("last_sink_write_at", lastSinkWriteAt)
                .put("last_error_at", lastErrorAt);
    }
}
