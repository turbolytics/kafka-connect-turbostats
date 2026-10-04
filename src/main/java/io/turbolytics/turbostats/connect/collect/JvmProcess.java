package io.turbolytics.turbostats.connect.collect;

import io.turbolytics.turbostats.connect.wire.Memory;
import io.turbolytics.turbostats.connect.wire.ProcessInfo;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;

/** The worker JVM, as the bundle's process section. */
public final class JvmProcess {
    /** 16 random bytes per JVM: every bundle this worker sends carries the same id. */
    public static final String PROCESS_ID = newId();

    /**
     * Pools whose after-collection usage tracked a leak in the spike: G1's
     * old generation in steps, non-generational ZGC's heap exactly.
     * Parallel's old generation stayed flat through a 140 MiB leak.
     */
    static final Set<String> LIVE_POOLS = Set.of("G1 Old Gen", "ZHeap");

    private JvmProcess() {
    }

    public static ProcessInfo read(Path cgroupRoot, Path procStatus) {
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
        long limit = Cgroup.memoryLimit(cgroupRoot).orElse(-1);
        return new ProcessInfo(
                PROCESS_ID,
                null,
                Instant.ofEpochMilli(rt.getStartTime()),
                rt.getUptime() / 1000,
                rssBytes(procStatus),
                limit > 0 ? limit : null,
                memory());
    }

    static Memory memory() {
        MemoryMXBean m = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = m.getHeapMemoryUsage();
        MemoryUsage nonHeap = m.getNonHeapMemoryUsage();
        Map<String, Long> afterGc = new HashMap<>();
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            MemoryUsage u = pool.getCollectionUsage();
            if (pool.getType() == MemoryType.HEAP && u != null) {
                afterGc.put(pool.getName(), u.getUsed());
            }
        }
        Map<String, Long> counts = new HashMap<>();
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            counts.put(gc.getName(), Math.max(0, gc.getCollectionCount()));
        }
        long max = heap.getMax();
        return new Memory(
                "jvm",
                heap.getCommitted() + nonHeap.getCommitted(),
                liveBytes(afterGc),
                max > 0 ? max : null,
                gcCount(counts));
    }

    /** After-collection usage of the verified pools, or null under any other collector. */
    static Long liveBytes(Map<String, Long> afterGcByPool) {
        Long sum = null;
        for (Map.Entry<String, Long> e : afterGcByPool.entrySet()) {
            if (LIVE_POOLS.contains(e.getKey())) {
                sum = (sum == null ? 0 : sum) + e.getValue();
            }
        }
        return sum;
    }

    /** Collections, without the beans that count pauses within a collection. */
    static long gcCount(Map<String, Long> countsByCollector) {
        long n = 0;
        for (Map.Entry<String, Long> e : countsByCollector.entrySet()) {
            if (!e.getKey().endsWith("Pauses")) {
                n += e.getValue();
            }
        }
        return n;
    }

    /** VmRSS from /proc/self/status; null off Linux or when unreadable. */
    static Long rssBytes(Path procStatus) {
        try {
            for (String line : Files.readAllLines(procStatus)) {
                if (line.startsWith("VmRSS:")) {
                    String[] parts = line.substring(6).trim().split("\\s+");
                    return Long.parseLong(parts[0]) * 1024;
                }
            }
        } catch (IOException | RuntimeException e) {
            return null;
        }
        return null;
    }

    private static String newId() {
        byte[] b = new byte[16];
        new SecureRandom().nextBytes(b);
        return HexFormat.of().formatHex(b);
    }
}
