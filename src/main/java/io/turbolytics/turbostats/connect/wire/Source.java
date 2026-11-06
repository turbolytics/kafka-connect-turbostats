package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;

/**
 * The database a source task reads, from its connector's config. It holds no
 * credential. A field the reporter could not read is null and leaves no key, so the
 * receiver can say the source cannot be named. Port is a JSON integer: a string
 * would fail the receiver's decode of the whole report.
 *
 * <p>The three settings decide whether the slot advances while the captured tables are
 * quiet. A heartbeat that is off is 0 and is sent. Whether a heartbeat query is set is
 * sent, never the query.
 */
public record Source(String kind, String host, Long port, String database, String slot,
        String lsnFlushMode, Long heartbeatIntervalMs, Boolean heartbeatActionQuery) {
    /** A source without its settings, as a connector the reporter can't read them from sends. */
    public Source(String kind, String host, Long port, String database, String slot) {
        this(kind, host, port, database, slot, null, null, null);
    }

    public JsonObject toJson() {
        return new JsonObject()
                .put("kind", kind)
                .put("host", host)
                .put("port", port)
                .put("database", database)
                .put("slot", slot)
                .put("lsn_flush_mode", lsnFlushMode)
                .put("heartbeat_interval_ms", heartbeatIntervalMs)
                .put("heartbeat_action_query", heartbeatActionQuery);
    }
}
