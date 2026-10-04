package io.turbolytics.turbostats.connect.collect;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

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
     * HMAC-SHA256 over the sorted key=value lines, keyed by the install's
     * credential. The config carries secrets in plain text, so only the hash
     * leaves the worker; it still changes when any setting does, which is
     * what a receiver needs. Keyed, it means nothing outside this install,
     * and nobody without the credential can test a guessed password
     * against it.
     */
    public static String configHash(Map<String, String> config, byte[] key) {
        StringBuilder b = new StringBuilder();
        new TreeMap<>(config).forEach((k, v) -> b.append(k).append('=').append(v).append('\n'));
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return "hmac-sha256:" + HexFormat.of().formatHex(mac.doFinal(b.toString().getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
