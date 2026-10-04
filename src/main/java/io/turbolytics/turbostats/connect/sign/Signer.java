package io.turbolytics.turbostats.connect.sign;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/** Signs a request the way turbostats/wire/sign.go does. */
public final class Signer {
    public static final String MEDIA_TYPE = "application/vnd.turbolytics.turbostats.v1+json";
    public static final String HEADER_KEY_ID = "X-Turbostats-Key-Id";
    public static final String HEADER_TIMESTAMP = "X-Turbostats-Timestamp";
    public static final String HEADER_SIGNATURE = "X-Turbostats-Signature";

    private Signer() {
    }

    /** The text a signature covers. The body is hashed, not included. */
    public static String canonical(String method, String path, long unixSeconds, byte[] body) {
        try {
            byte[] sum = MessageDigest.getInstance("SHA-256").digest(body);
            return "v1\n" + method + "\n" + path + "\n" + unixSeconds + "\n" + HexFormat.of().formatHex(sum);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * HTTP sends "/" for a URL written without a path, and the receiver
     * verifies against what it reads, so the empty path signs as "/".
     */
    public static String requestPath(String rawPath) {
        return rawPath == null || rawPath.isEmpty() ? "/" : rawPath;
    }

    public static Map<String, String> headers(Credential c, String method, String path, long unixSeconds, byte[] body) {
        byte[] sig = c.sign(canonical(method, path, unixSeconds, body).getBytes(StandardCharsets.UTF_8));
        Map<String, String> h = new LinkedHashMap<>();
        h.put(HEADER_KEY_ID, c.keyId());
        h.put(HEADER_TIMESTAMP, Long.toString(unixSeconds));
        h.put(HEADER_SIGNATURE, Base64.getEncoder().encodeToString(sig));
        return h;
    }
}
