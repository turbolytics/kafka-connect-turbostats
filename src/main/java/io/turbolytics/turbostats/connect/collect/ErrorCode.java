package io.turbolytics.turbostats.connect.collect;

import java.util.regex.Pattern;

/**
 * The code for a failed task: the exception class at the root of Connect's
 * trace. The root names the cause, such as a driver's exception, where the
 * top is usually Connect's own wrapper. The message never leaves the
 * worker: it can carry the row that failed or a connection string.
 */
public final class ErrorCode {
    // Qualified, as a trace names every class: a bare word on a "Caused by"
    // line is more likely text from a message than a class.
    private static final Pattern CLASS_NAME =
            Pattern.compile("[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+");
    static final int MAX_LENGTH = 128;

    private ErrorCode() {
    }

    /** The root cause's class name, or null when the trace names none. */
    public static String fromTrace(String trace) {
        if (trace == null || trace.isBlank()) {
            return null;
        }
        String root = null;
        for (String line : trace.split("\\R")) {
            String t = line.strip();
            if (root == null) {
                root = t;
            } else if (t.startsWith("Caused by: ")) {
                root = t.substring("Caused by: ".length());
            }
        }
        int end = root.indexOf(':');
        String name = (end < 0 ? root : root.substring(0, end)).strip();
        if (name.length() > MAX_LENGTH || !CLASS_NAME.matcher(name).matches()) {
            return null;
        }
        return name;
    }
}
