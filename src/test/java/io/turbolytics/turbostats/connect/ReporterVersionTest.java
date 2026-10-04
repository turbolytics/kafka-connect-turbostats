package io.turbolytics.turbostats.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ReporterVersionTest {
    // Tests run from classes, not the jar, so there is no manifest to read.
    @Test
    void outsideAJarTheVersionIsDev() {
        assertEquals("dev", ReporterVersion.get());
    }
}
