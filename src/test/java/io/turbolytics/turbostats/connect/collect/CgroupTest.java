package io.turbolytics.turbostats.connect.collect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CgroupTest {
    @TempDir
    Path root;

    Path write(String rel, String content) throws Exception {
        Path p = root.resolve(rel);
        Files.createDirectories(p.getParent());
        Files.writeString(p, content);
        return root;
    }

    // The same rules as SQLFlow's reader: no limit is not a limit of zero.
    @Test
    void readsV2AndV1AndTreatsUnlimitedAsAbsent() throws Exception {
        assertEquals(OptionalLong.of(536870912L), Cgroup.memoryLimit(write("memory.max", "536870912\n")));
    }

    @Test
    void v2MaxIsAbsent() throws Exception {
        assertTrue(Cgroup.memoryLimit(write("memory.max", "max\n")).isEmpty());
    }

    @Test
    void v1LimitAndItsUnlimitedSentinel() throws Exception {
        assertEquals(OptionalLong.of(536870912L), Cgroup.memoryLimit(write("memory/memory.limit_in_bytes", "536870912\n")));
    }

    @Test
    void v1SentinelIsAbsent() throws Exception {
        assertTrue(Cgroup.memoryLimit(write("memory/memory.limit_in_bytes", "9223372036854771712\n")).isEmpty());
    }

    @Test
    void noCgroupIsAbsent() {
        assertTrue(Cgroup.memoryLimit(root).isEmpty());
    }
}
