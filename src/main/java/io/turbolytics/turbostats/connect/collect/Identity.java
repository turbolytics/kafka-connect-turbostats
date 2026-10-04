package io.turbolytics.turbostats.connect.collect;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/** The instance fields the reporter derives rather than reads from settings. */
public final class Identity {
    private Identity() {
    }

    /** One stream of reports per task, unique within an org because the cluster is. */
    public static String instanceId(String cluster, TaskKey k) {
        return cluster + "/" + k.connector() + "/" + k.task();
    }

    /** The logical pipeline: a connector's tasks share it. */
    public static String instanceName(String cluster, String connector) {
        return cluster + "/" + connector;
    }

    /** os/arch in Go's spelling, as SQLFlow sends it, so a fleet groups by one value. */
    public static String arch(String osName, String osArch) {
        String os = osName == null ? "" : osName.toLowerCase(Locale.ROOT);
        if (os.startsWith("mac")) {
            os = "darwin";
        } else if (os.startsWith("windows")) {
            os = "windows";
        }
        String a = osArch == null ? "" : osArch.toLowerCase(Locale.ROOT);
        a = switch (a) {
            case "x86_64", "amd64" -> "amd64";
            case "aarch64", "arm64" -> "arm64";
            default -> a;
        };
        return os + "/" + a;
    }

    /**
     * The connector class's simple name, less its Connector suffix,
     * lowercased: PostgresConnector is postgres, JdbcSinkConnector is jdbc.
     * One rule rather than a table, so a connector nobody listed still gets
     * a readable type.
     */
    public static String typeOf(String connectorClass) {
        if (connectorClass == null || connectorClass.isBlank()) {
            return null;
        }
        String simple = connectorClass.substring(connectorClass.lastIndexOf('.') + 1);
        for (String suffix : new String[] {"SinkConnector", "SourceConnector", "Connector"}) {
            if (simple.endsWith(suffix) && simple.length() > suffix.length()) {
                simple = simple.substring(0, simple.length() - suffix.length());
                break;
            }
        }
        return simple.toLowerCase(Locale.ROOT);
    }

    /**
     * sha256 over the sorted key=value lines. The config carries secrets in
     * plain text, so only the hash leaves the worker; it still changes when
     * any setting does, which is what a receiver needs.
     */
    public static String configHash(Map<String, String> config) {
        StringBuilder b = new StringBuilder();
        new TreeMap<>(config).forEach((k, v) -> b.append(k).append('=').append(v).append('\n'));
        try {
            byte[] sum = MessageDigest.getInstance("SHA-256").digest(b.toString().getBytes(StandardCharsets.UTF_8));
            return "sha256:" + HexFormat.of().formatHex(sum);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
