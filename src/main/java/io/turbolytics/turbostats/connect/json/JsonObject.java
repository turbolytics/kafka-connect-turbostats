package io.turbolytics.turbostats.connect.json;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.TreeMap;

/**
 * A JSON object written field by field.
 *
 * Hand-written because the jar carries no dependencies: a JSON library in
 * /kafka/libs could shadow the one Kafka ships. The bundle is a fixed set of
 * fields, so writing is all it needs; nothing here parses.
 */
public final class JsonObject {
    private static final DateTimeFormatter TIME = DateTimeFormatter.ISO_INSTANT;

    private final StringBuilder b = new StringBuilder("{");
    private boolean empty = true;

    public JsonObject put(String key, String value) {
        if (value != null) {
            field(key).append(quote(value));
        }
        return this;
    }

    public JsonObject put(String key, long value) {
        field(key).append(value);
        return this;
    }

    public JsonObject put(String key, Long value) {
        if (value != null) {
            field(key).append(value.longValue());
        }
        return this;
    }

    public JsonObject put(String key, boolean value) {
        field(key).append(value);
        return this;
    }

    public JsonObject put(String key, Instant value) {
        if (value != null) {
            field(key).append(quote(formatTime(value)));
        }
        return this;
    }

    public JsonObject put(String key, JsonObject value) {
        if (value != null) {
            field(key).append(value.toJson());
        }
        return this;
    }

    /** Writes a string map as an object, keys sorted; absent when empty. */
    public JsonObject putMap(String key, Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return this;
        }
        JsonObject o = new JsonObject();
        new TreeMap<>(map).forEach(o::put);
        return put(key, o);
    }

    public String toJson() {
        return b + "}";
    }

    /** Whole seconds in UTC, the form Go's encoder writes a truncated time in. */
    public static String formatTime(Instant t) {
        return TIME.format(t.truncatedTo(ChronoUnit.SECONDS));
    }

    private StringBuilder field(String key) {
        if (!empty) {
            b.append(',');
        }
        empty = false;
        return b.append(quote(key)).append(':');
    }

    static String quote(String s) {
        StringBuilder q = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> q.append("\\\"");
                case '\\' -> q.append("\\\\");
                case '\n' -> q.append("\\n");
                case '\r' -> q.append("\\r");
                case '\t' -> q.append("\\t");
                case '\b' -> q.append("\\b");
                case '\f' -> q.append("\\f");
                default -> {
                    if (c < 0x20) {
                        q.append(String.format("\\u%04x", (int) c));
                    } else {
                        q.append(c);
                    }
                }
            }
        }
        return q.append('"').toString();
    }
}
