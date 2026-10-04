package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.wire.ProcessInfo;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JvmProcessTest {
    @TempDir
    Path dir;

    // The spike leaked 2 MiB/s under three collectors. ZGC's heap and G1's
    // old generation tracked it after collection; Parallel's old generation
    // stayed flat. A collector not verified sends no live_bytes, because a
    // flat reading during a leak is the worst kind of field.
    @Test
    void liveBytesOnlyUnderVerifiedCollectors() {
        assertEquals(287L << 20, JvmProcess.liveBytes(Map.of("G1 Eden Space", 0L, "G1 Old Gen", 287L << 20)));
        assertEquals(190L << 20, JvmProcess.liveBytes(Map.of("ZHeap", 190L << 20)));
        assertNull(JvmProcess.liveBytes(Map.of("PS Old Gen", 33L << 20, "PS Eden Space", 0L)));
        assertNull(JvmProcess.liveBytes(Map.of("Tenured Gen", 1L)));
        assertNull(JvmProcess.liveBytes(Map.of("ZGC Old Generation", 1L)));
    }

    // ZGC reports its pauses as a second collector; counting them would
    // count each cycle several times.
    @Test
    void gcCountLeavesOutPauseCounters() {
        assertEquals(44, JvmProcess.gcCount(Map.of("ZGC Cycles", 44L, "ZGC Pauses", 132L)));
        assertEquals(41, JvmProcess.gcCount(Map.of("G1 Young Generation", 35L, "G1 Concurrent GC", 6L, "G1 Old Generation", 0L)));
    }

    @Test
    void rssComesFromProcStatus() throws Exception {
        Path status = dir.resolve("status");
        Files.writeString(status, "Name:\tjava\nVmRSS:\t  891234 kB\nThreads:\t80\n");
        assertEquals(891234L * 1024, JvmProcess.rssBytes(status));
        assertNull(JvmProcess.rssBytes(dir.resolve("missing")));
    }

    @Test
    void readsThisJvm() {
        ProcessInfo p = JvmProcess.read(dir, dir.resolve("missing"));
        assertEquals(32, p.id().length());
        assertTrue(p.startedAt() != null);
        assertTrue(p.uptimeSeconds() >= 0);
        assertEquals("jvm", p.memory().runtime());
        assertTrue(p.memory().retainedBytes() > 0);
        assertNull(p.memoryLimitBytes());
        assertNull(p.host());
    }
}
