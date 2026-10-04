package io.turbolytics.turbostats.connect.collect;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;

/** The container's memory limit, read the way SQLFlow reads it. */
public final class Cgroup {
    public static final Path ROOT = Path.of("/sys/fs/cgroup");

    private Cgroup() {
    }

    /**
     * Empty when there is no cgroup filesystem, no limit, or a file this does
     * not understand. Read on every report, because an orchestrator can
     * resize a running container.
     */
    public static OptionalLong memoryLimit(Path root) {
        for (Path p : new Path[] {root.resolve("memory.max"), root.resolve("memory").resolve("memory.limit_in_bytes")}) {
            if (Files.isReadable(p)) {
                try {
                    return parse(Files.readString(p));
                } catch (IOException e) {
                    return OptionalLong.empty();
                }
            }
        }
        return OptionalLong.empty();
    }

    /** cgroup v1 spells unlimited as the largest page-aligned long; 2^62 and up is no limit. */
    static OptionalLong parse(String raw) {
        String s = raw.trim();
        if (s.equals("max")) {
            return OptionalLong.empty();
        }
        try {
            long n = Long.parseLong(s);
            return n <= 0 || n >= (1L << 62) ? OptionalLong.empty() : OptionalLong.of(n);
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }
}
