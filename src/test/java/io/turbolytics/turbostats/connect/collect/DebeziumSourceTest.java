package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.turbolytics.turbostats.connect.wire.Source;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DebeziumSourceTest {
    private static final String POSTGRES = "io.debezium.connector.postgresql.PostgresConnector";

    private static Map<String, String> config() {
        Map<String, String> c = new HashMap<>();
        c.put("connector.class", POSTGRES);
        c.put("database.hostname", "db.internal");
        c.put("database.port", "5433");
        c.put("database.dbname", "billing");
        c.put("slot.name", "debezium_billing");
        return c;
    }

    @Test
    void namesThePostgresSource() {
        assertEquals(new Source("postgres", "db.internal", 5433L, "billing", "debezium_billing"),
                DebeziumSource.of(config()));
    }

    // Debezium applies these defaults, so the connector really uses them. Leaving
    // them out would read as a source the reporter could not name.
    @Test
    void appliesDebeziumsDefaults() {
        Map<String, String> c = config();
        c.remove("database.port");
        c.remove("slot.name");
        Source s = DebeziumSource.of(c);
        assertEquals(5432L, s.port());
        assertEquals("debezium", s.slot());
    }

    @Test
    void anotherConnectorHasNoSource() {
        for (String cls : new String[] {"io.debezium.connector.mysql.MySqlConnector",
                "io.debezium.connector.jdbc.JdbcSinkConnector",
                "io.confluent.connect.jdbc.JdbcSourceConnector"}) {
            Map<String, String> c = config();
            c.put("connector.class", cls);
            assertNull(DebeziumSource.of(c), cls);
        }
        Map<String, String> none = config();
        none.remove("connector.class");
        assertNull(DebeziumSource.of(none));
    }

    // A placeholder can name a secret's path, so it is never sent. The source still
    // arrives, so Control can say it cannot name it rather than show nothing.
    @Test
    void dropsAPlaceholder() {
        Map<String, String> c = config();
        c.put("database.hostname", "${file:/secrets/db.properties:host}");
        c.put("database.port", "${env:DB_PORT}");
        c.put("database.dbname", "${vault:db}");
        c.put("slot.name", "x-${env:SLOT}");
        Source s = DebeziumSource.of(c);
        assertEquals("postgres", s.kind());
        assertNull(s.host());
        assertNull(s.port());
        assertNull(s.database());
        assertNull(s.slot());
    }

    // A string port fails the receiver's decode of the whole report, and Connect's
    // config values are all strings.
    @Test
    void aPortThatIsNotANumberIsAbsent() {
        for (String port : new String[] {"", " ", "abc", "54x", "0", "-1", "65536", "99999999999999999999"}) {
            Map<String, String> c = config();
            c.put("database.port", port);
            assertNull(DebeziumSource.of(c).port(), port);
        }
        Map<String, String> c = config();
        c.put("database.port", " 5432 ");
        assertEquals(5432L, DebeziumSource.of(c).port());
    }

    // These limits are what the names can really hold, so a value past them is not a
    // name, and it stays inside any bound the receiver sets.
    @Test
    void aValueTheReceiverWouldRefuseIsAbsent() {
        Map<String, String> c = config();
        c.put("database.hostname", "h".repeat(254));
        c.put("database.dbname", "d".repeat(64));
        c.put("slot.name", "s\u0000");
        Source s = DebeziumSource.of(c);
        assertNull(s.host());
        assertNull(s.database());
        assertNull(s.slot());

        Map<String, String> widest = config();
        widest.put("database.hostname", "h".repeat(253));
        widest.put("database.dbname", "d".repeat(63));
        widest.put("slot.name", "s".repeat(63));
        Source w = DebeziumSource.of(widest);
        assertNotNull(w.host());
        assertNotNull(w.database());
        assertNotNull(w.slot());
    }

    // 63 is Postgres's limit in bytes, not characters.
    @Test
    void aNameIsMeasuredInBytes() {
        Map<String, String> c = config();
        c.put("database.dbname", "é".repeat(32));
        assertNull(DebeziumSource.of(c).database());
    }

    @Test
    void aBlankValueIsAbsent() {
        Map<String, String> c = config();
        c.put("database.hostname", "  ");
        assertNull(DebeziumSource.of(c).host());
    }

    // Debezium builds its JDBC URL by substituting these values, so a dbname can
    // carry a password and still connect. The value is read by name, so its shape is
    // the only guard against what's inside it.
    @Test
    void aValueThatCouldCarryACredentialIsAbsent() {
        for (String dbname : new String[] {"billing?password=hunter2", "billing&user=x", "a=b", "bill%3Fing", "a/b"}) {
            Map<String, String> c = config();
            c.put("database.dbname", dbname);
            assertNull(DebeziumSource.of(c).database(), dbname);
        }
        for (String host : new String[] {"user:hunter2@db.internal", "db1,db2", "db.internal/billing", "db internal", "db?x=y"}) {
            Map<String, String> c = config();
            c.put("database.hostname", host);
            assertNull(DebeziumSource.of(c).host(), host);
        }
    }

    @Test
    void aRealHostIsKept() {
        for (String host : new String[] {"db.internal", "DB-1.Internal", "10.0.3.14", "[2001:db8::1]", "2001:db8::1", "db_1"}) {
            Map<String, String> c = config();
            c.put("database.hostname", host);
            assertEquals(host, DebeziumSource.of(c).host());
        }
    }

    // Debezium refuses any other slot name, so another shape never runs.
    @Test
    void aSlotDebeziumWouldRefuseIsAbsent() {
        for (String slot : new String[] {"Upper", "has-dash", "a b", "s".repeat(64)}) {
            Map<String, String> c = config();
            c.put("slot.name", slot);
            assertNull(DebeziumSource.of(c).slot(), slot);
        }
    }

    @Test
    void aControlCharacterIsAbsent() {
        for (String bad : new String[] {"a\nb", "a\tb", "a\u007fb", "a\u0000b"}) {
            Map<String, String> c = config();
            c.put("database.dbname", bad);
            assertNull(DebeziumSource.of(c).database(), bad);
        }
    }

    // Connect accepts a connector class's simple name and its short alias. Missing
    // one would read as a connector with no database, and show no slot at all.
    @Test
    void anAliasIsStillPostgres() {
        for (String cls : new String[] {POSTGRES, "PostgresConnector", "Postgres"}) {
            Map<String, String> c = config();
            c.put("connector.class", cls);
            assertNotNull(DebeziumSource.of(c), cls);
        }
    }

    // The config holds a password. Nothing but the four named fields may leave.
    @Test
    void sendsNothingElse() {
        Map<String, String> c = config();
        c.put("database.password", "hunter2");
        c.put("database.user", "replicator");
        c.put("database.sslkey", "/secrets/key.pem");
        c.put("table.include.list", "public.invoices");
        String json = DebeziumSource.of(c).toJson().toJson();
        assertEquals("{\"kind\":\"postgres\",\"host\":\"db.internal\",\"port\":5433,"
                + "\"database\":\"billing\",\"slot\":\"debezium_billing\"}", json);
    }

    @Test
    void anUnreadableFieldLeavesNoKey() {
        Map<String, String> c = config();
        c.put("database.hostname", "${env:HOST}");
        c.put("database.port", "abc");
        assertEquals("{\"kind\":\"postgres\",\"database\":\"billing\",\"slot\":\"debezium_billing\"}",
                DebeziumSource.of(c).toJson().toJson());
    }
}
