package io.turbolytics.turbostats.connect.wire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class BundleSchemaTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static Set<ValidationMessage> validate(String json) throws Exception {
        try (InputStream in = BundleSchemaTest.class.getResourceAsStream("/schema/bundle.schema.json")) {
            JsonSchema schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(in);
            return schema.validate(MAPPER.readTree(json));
        }
    }

    // Every bundle shape the reporter sends must be one the contract
    // accepts. A Java reporter cannot share the Go types, so the published
    // schema is what holds the two together.
    @Test
    void aSourceTaskBundleValidates() throws Exception {
        assertEquals(Set.of(), validate(Fixtures.sourceBundle().toJson()));
    }

    @Test
    void aSinkTaskBundleValidates() throws Exception {
        assertEquals(Set.of(), validate(Fixtures.sinkBundle().toJson()));
    }

    @Test
    void anExitBundleValidatesAndSaysStopped() throws Exception {
        Bundle exit = Fixtures.sourceBundle().withExit(new Exit("connector_deleted", 0), Fixtures.T.plusSeconds(60));
        assertEquals(Set.of(), validate(exit.toJson()));
        JsonNode doc = MAPPER.readTree(exit.toJson());
        assertEquals("stopped", doc.at("/pipeline/state").asText());
        assertEquals("connector_deleted", doc.at("/exit/reason").asText());
    }

    // Review focus: operator text in a label must not break the document.
    @Test
    void aLabelWithEscapesValidates() throws Exception {
        Bundle b = Fixtures.sourceBundle();
        Bundle hostile = new Bundle(b.sentAt(), b.intervalSeconds(), b.lastActivityAt(),
                Fixtures.instance("postgres", "kafka", Map.of("tenant", "a\"b\\c\n東京")),
                b.process(), b.pipeline(), null);
        assertEquals(Set.of(), validate(hostile.toJson()));
    }

    // A source task has no flush count. The key is absent, not zero.
    @Test
    void aSourceTaskSendsNoFlushCount() throws Exception {
        JsonNode doc = MAPPER.readTree(Fixtures.sourceBundle().toJson());
        assertFalse(doc.get("pipeline").has("sink_flush_count"));
        assertFalse(doc.get("process").has("goroutines"));
        assertEquals("kafka-connect", doc.at("/instance/runtime").asText());
    }

    // The schema is not vacuous: a pipeline without a required counter fails.
    @Test
    void aBundleMissingARequiredCounterIsRejected() throws Exception {
        ObjectNode doc = (ObjectNode) MAPPER.readTree(Fixtures.sourceBundle().toJson());
        ((ObjectNode) doc.get("pipeline")).remove("sink_rows_written");
        assertTrue(validate(doc.toString()).size() > 0);
    }
}
