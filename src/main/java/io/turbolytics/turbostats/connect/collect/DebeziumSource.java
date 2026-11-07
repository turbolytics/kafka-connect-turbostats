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
 *
 * <p>It also reads the settings that decide whether the slot advances while the tables
 * it captures are quiet: lsn.flush.mode, or flush.lsn.source before 3.4, the heartbeat's
 * interval, and whether a heartbeat query is set. A setting it cannot read is absent,
 * never Debezium's default, which would name the wrong fix.
 */
public final class DebeziumSource {
    // Connect accepts a class's full name, its simple name, or that less "Connector".
    private static final Set<String> POSTGRES = Set.of(
            "io.debezium.connector.postgresql.PostgresConnector", "PostgresConnector", "Postgres");
    // Debezium's defaults, which the connector really uses when these are unset.
    private static final String DEFAULT_PORT = "5432";
    private static final String DEFAULT_SLOT = "debezium";
    private static final Set<String> FLUSH_MODES = Set.of("connector", "connector_and_driver", "manual");
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
                slot != null && SLOT.matcher(slot).matches() ? slot : null,
                flushMode(config), heartbeat(config.get("heartbeat.interval.ms")),
                actionQuery(config.get("heartbeat.action.query")));
    }

    /**
     * Debezium's lsn.flush.mode. When it is unset, 3.4 reads the deprecated
     * flush.lsn.source, as every version before 3.4 did: true, the default, is
     * connector, and false is manual.
     */
    private static String flushMode(Map<String, String> config) {
        String mode = config.get("lsn.flush.mode");
        if (mode != null && !mode.isBlank()) {
            String v = readable(mode, MAX_NAME);
            return v != null && FLUSH_MODES.contains(v.toLowerCase(java.util.Locale.ROOT))
                    ? v.toLowerCase(java.util.Locale.ROOT) : null;
        }
        String source = config.get("flush.lsn.source");
        if (source == null || source.isBlank()) {
            return "connector";
        }
        String v = readable(source, MAX_NAME);
        if ("true".equalsIgnoreCase(v)) {
            return "connector";
        }
        return "false".equalsIgnoreCase(v) ? "manual" : null;
    }

    /** Milliseconds between heartbeats. Unset is Debezium's default, 0, which is off. */
    private static Long heartbeat(String value) {
        if (value == null || value.isBlank()) {
            return 0L;
        }
        String v = readable(value, MAX_NAME);
        if (v == null) {
            return null;
        }
        try {
            long ms = Long.parseLong(v);
            return ms >= 0 ? ms : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** Whether a heartbeat query is set. The query itself never leaves: it can carry anything. */
    private static Boolean actionQuery(String value) {
        return value != null && !value.isBlank();
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
