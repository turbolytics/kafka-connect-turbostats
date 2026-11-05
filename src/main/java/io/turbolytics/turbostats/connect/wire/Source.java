package io.turbolytics.turbostats.connect.wire;

import io.turbolytics.turbostats.connect.json.JsonObject;

/**
 * The database a source task reads, from its connector's config. It holds no
 * credential. A field the reporter could not read is null and leaves no key, so the
 * receiver can say the source cannot be named. Port is a JSON integer: a string
 * would fail the receiver's decode of the whole report.
 */
public record Source(String kind, String host, Long port, String database, String slot) {
    public JsonObject toJson() {
        return new JsonObject()
                .put("kind", kind)
                .put("host", host)
                .put("port", port)
                .put("database", database)
                .put("slot", slot);
    }
}
