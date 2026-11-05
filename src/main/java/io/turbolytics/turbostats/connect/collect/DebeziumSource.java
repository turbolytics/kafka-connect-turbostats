package io.turbolytics.turbostats.connect.collect;

import io.turbolytics.turbostats.connect.wire.Source;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * What a Debezium Postgres connector's config says about the database it reads:
 * enough for the receiver to find the replication slot it holds. A config holds
 * passwords, so this reads these four fields by name and nothing else, and sends a
 * value only when its shape can't carry anything but what it names.
 */
public final class DebeziumSource {
    // Connect accepts a class's full name, its simple name, or that less "Connector".
    private static final Set<String> POSTGRES = Set.of(
            "io.debezium.connector.postgresql.PostgresConnector", "PostgresConnector", "Postgres");
    // Debezium's defaults, which the connector really uses when these are unset.
    private static final String DEFAULT_PORT = "5432";
    private static final String DEFAULT_SLOT = "debezium";
    // What each value can be. Debezium builds its JDBC URL by substituting them, so a
    // dbname can carry a password and still connect, and shape is the only guard on
    // what is inside a value read by name.
    private static final Pattern HOST = Pattern.compile("[A-Za-z0-9._:\\[\\]-]+");
    private static final Pattern NOT_A_DATABASE = Pattern.compile("[?&=%/;]");
    private static final Pattern SLOT = Pattern.compile("[a-z0-9_]{1,63}");
    // A DNS name's limit, and a Postgres name's, in bytes. Past them a value is not a
    // name, and this keeps it inside any bound the receiver sets.
    private static final int MAX_HOST = 253;
    private static final int MAX_NAME = 63;

    private DebeziumSource() {
    }

    /** The connector's source, or null for any connector but Debezium's for Postgres. */
    public static Source of(Map<String, String> config) {
        if (!POSTGRES.contains(config.getOrDefault("connector.class", ""))) {
            return null;
        }
        String host = readable(config.get("database.hostname"), MAX_HOST);
        String database = readable(config.get("database.dbname"), MAX_NAME);
        String slot = readable(config.getOrDefault("slot.name", DEFAULT_SLOT), MAX_NAME);
        return new Source("postgres",
                host != null && HOST.matcher(host).matches() ? host : null,
                port(config.getOrDefault("database.port", DEFAULT_PORT)),
                database != null && !NOT_A_DATABASE.matcher(database).find() ? database : null,
                slot != null && SLOT.matcher(slot).matches() ? slot : null);
    }

    /**
     * The value, or null when it cannot go on the wire: absent, blank, a placeholder
     * such as ${file:...}, which can name a secret's path, past its limit, or holding
     * a control character.
     */
    private static String readable(String value, int maxBytes) {
        if (value == null) {
            return null;
        }
        String v = value.strip();
        if (v.isEmpty() || v.contains("${") || v.getBytes(StandardCharsets.UTF_8).length > maxBytes
                || v.chars().anyMatch(Character::isISOControl)) {
            return null;
        }
        return v;
    }

    private static Long port(String value) {
        String v = readable(value, MAX_NAME);
        if (v == null) {
            return null;
        }
        try {
            long p = Long.parseLong(v);
            return p >= 1 && p <= 65535 ? p : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
