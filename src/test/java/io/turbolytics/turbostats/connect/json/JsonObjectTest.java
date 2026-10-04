package io.turbolytics.turbostats.connect.json;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonObjectTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Absent and zero are different facts in the contract, so null writes
    // no key at all.
    @Test
    void aNullValueWritesNoKey() {
        String nothing = null;
        Long none = null;
        assertEquals("{\"a\":1}", new JsonObject().put("a", 1L).put("b", nothing).put("c", none).toJson());
    }

    @Test
    void anEmptyObjectIsBraces() {
        assertEquals("{}", new JsonObject().toJson());
    }

    // A label value is operator text. Quotes, backslashes, control
    // characters and non-ASCII text must survive a round trip.
    @Test
    void stringsAreEscaped() throws Exception {
        String hostile = "a\"b\\c\nd\te\u0001f é 東京";
        String json = new JsonObject().put("k", hostile).toJson();
        JsonNode parsed = MAPPER.readTree(json);
        assertEquals(hostile, parsed.get("k").asText());
    }

    @Test
    void timesAreWholeSecondsInUtc() {
        Instant t = Instant.parse("2026-10-04T10:00:00.987Z");
        assertEquals("{\"t\":\"2026-10-04T10:00:00Z\"}", new JsonObject().put("t", t).toJson());
    }

    // Labels are written sorted, so the same labels always make the same
    // bytes; an empty map is absent, like an unset field.
    @Test
    void mapsAreSortedAndAnEmptyMapIsAbsent() {
        assertEquals("{\"labels\":{\"a\":\"1\",\"b\":\"2\"}}",
                new JsonObject().putMap("labels", Map.of("b", "2", "a", "1")).toJson());
        assertEquals("{}", new JsonObject().putMap("labels", Map.of()).toJson());
    }

    @Test
    void nestedObjectsAndBooleans() {
        assertEquals("{\"o\":{\"x\":true}}", new JsonObject().put("o", new JsonObject().put("x", true)).toJson());
    }
}
