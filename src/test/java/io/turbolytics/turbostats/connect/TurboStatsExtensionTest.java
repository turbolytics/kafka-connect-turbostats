package io.turbolytics.turbostats.connect;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TurboStatsExtensionTest {
    // A worker's REST server stops if configure throws. Nothing it is handed
    // may make it throw.
    @Test
    void configureNeverThrows() {
        Map<String, Object> garbage = new HashMap<>();
        garbage.put("turbostats.report.to", "http://public.example.com");
        garbage.put("turbostats.key", "nope");
        garbage.put("turbostats.label.Bad", "x");
        TurboStatsExtension e = new TurboStatsExtension();
        assertDoesNotThrow(() -> e.configure(garbage));
        assertDoesNotThrow(() -> e.configure(null));
        assertDoesNotThrow(() -> e.register(null));
        assertDoesNotThrow(e::close);
    }

    /** Stands in for Connect's isolated plugin loader, matched by name. */
    static final class PluginClassLoader extends ClassLoader {
    }

    // In the plugin path, the interceptors cannot load for any task, and
    // every source task on the worker fails. The extension says so loudly.
    @Test
    void detectsAPluginPathInstall() {
        assertTrue(TurboStatsExtension.loadedFromPluginPath(new PluginClassLoader()));
        assertFalse(TurboStatsExtension.loadedFromPluginPath(ClassLoader.getSystemClassLoader()));
        assertFalse(TurboStatsExtension.loadedFromPluginPath(null));
    }

    @Test
    void theServiceFileNamesTheExtension() throws Exception {
        try (var in = getClass().getResourceAsStream(
                "/META-INF/services/org.apache.kafka.connect.rest.ConnectRestExtension")) {
            assertTrue(new String(in.readAllBytes()).trim().equals(TurboStatsExtension.class.getName()));
        }
    }
}
