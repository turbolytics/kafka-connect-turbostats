package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.turbolytics.turbostats.connect.wire.Backfill;
import io.turbolytics.turbostats.connect.wire.EventLag;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DebeziumMapTest {
    static final Instant NOW = Instant.parse("2026-10-05T10:00:00Z");

    static DebeziumMetrics.View view(List<Map<String, Object>> snap, boolean snapStale,
            List<Map<String, Object>> stream, boolean streamStale) {
        return new DebeziumMetrics.View(snap, snapStale, stream, streamStale);
    }

    // The spike's initial snapshot: running, no chunk, streaming not yet open.
    @Test
    void anInitialSnapshotBlocksTheStream() {
        Backfill b = DebeziumMetrics.backfill(view(List.of(Map.of("SnapshotRunning", true, "TotalTableCount", 3,
                "RemainingTableCount", 2, "SnapshotDurationInSeconds", 39L)), false, List.of(), false));
        assertEquals("running", b.state());
        assertTrue(b.blocksStream());
        assertEquals(3, b.unitsTotal());
        assertEquals(2, b.unitsLeft());
        assertEquals("table", b.unit());
        assertEquals(39L, b.elapsedSeconds());
    }

    // An incremental snapshot runs beside the stream: ChunkId is set.
    @Test
    void anIncrementalSnapshotDoesNotBlock() {
        Backfill b = DebeziumMetrics.backfill(view(List.of(Map.of("SnapshotRunning", true, "ChunkId", "6d0d")),
                false, List.of(), false));
        assertFalse(b.blocksStream());
    }

    @Test
    void missingOrStaleSnapshotMetricsAreUnknown() {
        assertEquals("unknown", DebeziumMetrics.backfill(view(List.of(), false, List.of(), false)).state());
        assertEquals("unknown", DebeziumMetrics.backfill(view(List.of(Map.of("SnapshotRunning", true)), true,
                List.of(), false)).state());
    }

    // Fresh after a completed snapshot: all zeros, neither flag set.
    @Test
    void noSnapshotThisRunIsNone() {
        assertEquals("none", DebeziumMetrics.backfill(view(List.of(Map.of("SnapshotRunning", false,
                "SnapshotCompleted", false)), false, List.of(), false)).state());
    }

    @Test
    void eventLagIsMillisecondsBehindSourceWithItsBasis() {
        EventLag l = DebeziumMetrics.eventLag(view(List.of(), false, List.of(Map.of("MilliSecondsBehindSource", 1500L,
                "MilliSecondsBehindSourceMaxValue", 9000L, "MilliSecondsSinceLastEvent", 2000L)), false), NOW);
        assertEquals(1.5, l.seconds());
        assertEquals(9.0, l.maxSeconds());
        assertEquals(NOW.minusMillis(2000), l.observedAt());
        assertEquals("source_commit_time", l.basis());
    }

    // Review focus: -1 means no event yet.
    @Test
    void noEventYetIsNoLag() {
        assertNull(DebeziumMetrics.eventLag(view(List.of(), false, List.of(Map.of("MilliSecondsBehindSource", -1L)),
                false), NOW));
    }

    // A database that is not yet streamed to cannot be lost.
    @Test
    void connectionIsAbsentWhileTheSnapshotBlocksOrWhenStale() {
        DebeziumMetrics.View v = view(List.of(), false, List.of(Map.of("Connected", false)), false);
        Backfill blocking = new Backfill("running", true, null, "table", null, null, null);
        assertNull(DebeziumMetrics.sourceConnected(v, blocking));
        assertFalse(DebeziumMetrics.sourceConnected(v, Backfill.none()));
        assertNull(DebeziumMetrics.sourceConnected(view(List.of(), false, List.of(Map.of("Connected", true)), true),
                Backfill.none()));
    }

    // Review: an unreadable Connected is no reading, not a disconnect.
    @Test
    void anUnreadableConnectionIsAbsent() {
        assertNull(DebeziumMetrics.sourceConnected(view(List.of(), false, List.of(Map.of()), false), Backfill.none()));
    }

    // Per-database MBeans are shards: lag is the worst.
    @Test
    void databasesCollapseAsShards() {
        EventLag l = DebeziumMetrics.eventLag(view(List.of(), false, List.of(
                Map.of("MilliSecondsBehindSource", 100L), Map.of("MilliSecondsBehindSource", 700L)), false), NOW);
        assertEquals(0.7, l.seconds());
    }
}
