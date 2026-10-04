package io.turbolytics.turbostats.connect.config;

import io.turbolytics.turbostats.connect.sign.Credential;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * The reporter's settings, from the worker's turbostats.* properties.
 *
 * parse never throws. An exception out of the extension's configure stops
 * the worker's REST server, and a monitoring jar must never be the reason a
 * worker is down; a bad setting is an error message and reporting stays off.
 */
public record ReporterConfig(
        URI reportTo,
        Credential credential,
        String cluster,
        int intervalSeconds,
        int timeoutSeconds,
        Map<String, String> labels) {

    public static final String PREFIX = "turbostats.";
    public static final String LABEL_PREFIX = "turbostats.label.";
    public static final int DEFAULT_INTERVAL_SECONDS = 60;
    public static final int DEFAULT_TIMEOUT_SECONDS = 10;
    public static final int MAX_LABELS = 10;
    public static final int MAX_LABEL_KEY = 32;
    public static final int MAX_LABEL_VALUE = 64;
    public static final int MAX_CLUSTER = 64;

    /** The instance field names a label may not shadow, as SQLFlow refuses them. */
    public static final Set<String> RESERVED_LABELS = Set.of("id", "name", "version", "commit", "arch",
            "config_hash", "source_type", "sink_type", "handler_type", "runtime", "runtime_version",
            "reporter_version");

    /** group.id values that installs keep from examples, and so collide across clusters. */
    static final Set<String> STOCK_GROUP_IDS = Set.of("connect-cluster", "connect", "compose-connect-group");

    private static final Pattern LABEL_KEY = Pattern.compile("[a-z][a-z0-9_]*");

    public record Parsed(ReporterConfig config, List<String> errors, List<String> warnings) {
        public boolean enabled() {
            return config != null;
        }
    }

    public static Parsed parse(Map<String, ?> props) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        try {
            return parse(props == null ? Map.of() : props, errors, warnings);
        } catch (RuntimeException e) {
            errors.add("turbostats settings could not be read: " + e.getClass().getSimpleName());
            return new Parsed(null, errors, warnings);
        }
    }

    private static Parsed parse(Map<String, ?> props, List<String> errors, List<String> warnings) {
        String reportTo = string(props, "turbostats.report.to");
        if (reportTo == null || reportTo.isBlank()) {
            return new Parsed(null, List.of(), List.of());
        }
        URI uri = destination(reportTo, errors);

        Credential credential = null;
        String key = string(props, "turbostats.key");
        if (key == null || key.isBlank()) {
            errors.add("turbostats.key is required with turbostats.report.to");
        } else {
            try {
                credential = Credential.parse(key);
            } catch (IllegalArgumentException e) {
                errors.add("turbostats.key: " + e.getMessage());
            }
        }

        String cluster = string(props, "turbostats.cluster");
        if (cluster == null || cluster.isBlank()) {
            cluster = string(props, "group.id");
            if (cluster != null && STOCK_GROUP_IDS.contains(cluster)) {
                warnings.add("turbostats.cluster defaults to group.id \"" + cluster
                        + "\", which many installs share; set turbostats.cluster so instance ids stay unique");
            }
        }
        if (cluster == null || cluster.isBlank()) {
            errors.add("turbostats.cluster is required when the worker has no group.id");
        } else if (cluster.contains("/") || cluster.length() > MAX_CLUSTER) {
            errors.add("turbostats.cluster may not contain / and is at most " + MAX_CLUSTER + " characters");
        }

        int interval = positiveInt(props, "turbostats.interval.seconds", DEFAULT_INTERVAL_SECONDS, errors);
        int timeout = positiveInt(props, "turbostats.timeout.seconds", DEFAULT_TIMEOUT_SECONDS, errors);
        if (interval > 0 && timeout > 0 && timeout >= interval) {
            errors.add("turbostats.timeout.seconds must be less than turbostats.interval.seconds");
        }

        Map<String, String> labels = labels(props, errors);

        if (!errors.isEmpty()) {
            return new Parsed(null, errors, warnings);
        }
        return new Parsed(new ReporterConfig(uri, credential, cluster, interval, timeout, labels), errors, warnings);
    }

    private static URI destination(String raw, List<String> errors) {
        URI uri;
        try {
            uri = URI.create(raw.trim());
        } catch (IllegalArgumentException e) {
            errors.add("turbostats.report.to is not a URL");
            return null;
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (uri.getHost() == null) {
            errors.add("turbostats.report.to has no host");
        } else if (scheme.equals("http")) {
            if (!loopback(uri.getHost())) {
                errors.add("turbostats.report.to is plaintext to a public address; use https");
            }
        } else if (!scheme.equals("https")) {
            errors.add("turbostats.report.to must be http or https");
        }
        return uri;
    }

    /** Literal addresses only: a name other than localhost would need DNS to judge. */
    static boolean loopback(String host) {
        String h = host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
        if (h.equalsIgnoreCase("localhost")) {
            return true;
        }
        if (!h.matches("[0-9.]+") && !h.contains(":")) {
            return false;
        }
        try {
            return InetAddress.getByName(h).isLoopbackAddress();
        } catch (Exception e) {
            return false;
        }
    }

    private static Map<String, String> labels(Map<String, ?> props, List<String> errors) {
        Map<String, String> out = new TreeMap<>();
        for (Map.Entry<String, ?> e : props.entrySet()) {
            if (e.getKey() == null || !e.getKey().startsWith(LABEL_PREFIX)) {
                continue;
            }
            String k = e.getKey().substring(LABEL_PREFIX.length());
            String v = e.getValue() == null ? "" : String.valueOf(e.getValue());
            if (!LABEL_KEY.matcher(k).matches() || k.length() > MAX_LABEL_KEY) {
                errors.add("label key \"" + k + "\" must match [a-z][a-z0-9_]* and be at most " + MAX_LABEL_KEY
                        + " characters");
            } else if (RESERVED_LABELS.contains(k)) {
                errors.add("label key \"" + k + "\" is a field the report already carries");
            } else if (v.length() > MAX_LABEL_VALUE) {
                errors.add("label \"" + k + "\" is longer than " + MAX_LABEL_VALUE + " characters");
            } else {
                out.put(k, v);
            }
        }
        if (out.size() > MAX_LABELS) {
            errors.add("at most " + MAX_LABELS + " labels");
        }
        return Collections.unmodifiableMap(out);
    }

    private static int positiveInt(Map<String, ?> props, String name, int dflt, List<String> errors) {
        String raw = string(props, name);
        if (raw == null || raw.isBlank()) {
            return dflt;
        }
        try {
            int v = Integer.parseInt(raw.trim());
            if (v < 1) {
                errors.add(name + " must be at least 1");
                return -1;
            }
            return v;
        } catch (NumberFormatException e) {
            errors.add(name + " is not a whole number of seconds");
            return -1;
        }
    }

    private static String string(Map<String, ?> props, String name) {
        Object v = props.get(name);
        return v == null ? null : String.valueOf(v);
    }
}
