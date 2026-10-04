package io.turbolytics.turbostats.connect.sign;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SignerTest {
    static JsonNode vectors() throws Exception {
        try (InputStream in = SignerTest.class.getResourceAsStream("/vectors.json")) {
            return new ObjectMapper().readTree(in);
        }
    }

    // The Go reference produced these bytes. A signature that differs by
    // one byte is refused by every control plane.
    @Test
    void signsExactlyAsTheGoReference() throws Exception {
        JsonNode v = vectors();
        Credential c = Credential.parse(v.get("credential").asText());
        assertEquals(v.get("public_key_hex").asText(), HexFormat.of().formatHex(c.publicKey()));
        assertEquals(v.get("key_id").asText(), c.keyId());

        byte[] body = v.get("body").asText().getBytes(StandardCharsets.UTF_8);
        String canonical = Signer.canonical(v.get("method").asText(), v.get("path").asText(),
                v.get("timestamp").asLong(), body);
        assertTrue(canonical.endsWith(v.get("body_sha256").asText()));

        Map<String, String> h = Signer.headers(c, v.get("method").asText(), v.get("path").asText(),
                v.get("timestamp").asLong(), body);
        assertEquals(v.get("key_id").asText(), h.get("X-Turbostats-Key-Id"));
        assertEquals(v.get("timestamp").asText(), h.get("X-Turbostats-Timestamp"));
        assertEquals(v.get("signature_b64").asText(), h.get("X-Turbostats-Signature"));
    }

    @Test
    void anEmptyPathSignsAsSlash() {
        assertEquals("/", Signer.requestPath(""));
        assertEquals("/v1/turbostats", Signer.requestPath("/v1/turbostats"));
    }

    // A bad credential names what is wrong and never echoes the secret.
    @Test
    void aBadCredentialNeverEchoesItself() {
        String secret = "sfc_not!base64url!at!all";
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Credential.parse(secret));
        assertFalse(e.getMessage().contains("not!base64url"));

        String publicHalf = "sfp_A6EHv_POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg";
        IllegalArgumentException p = assertThrows(IllegalArgumentException.class, () -> Credential.parse(publicHalf));
        assertTrue(p.getMessage().contains("public"));

        String shortSeed = "sfc_" + Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[16]);
        assertThrows(IllegalArgumentException.class, () -> Credential.parse(shortSeed));
    }

    // The config hash key derives from the credential: stable for one
    // install, different across installs, and never the seed itself.
    @Test
    void theConfigHashKeyIsStableAndNotTheSeed() throws Exception {
        String key = vectors().get("credential").asText();
        byte[] k1 = Credential.parse(key).configHashKey();
        byte[] k2 = Credential.parse(key).configHashKey();
        assertEquals(HexFormat.of().formatHex(k1), HexFormat.of().formatHex(k2));
        assertFalse(HexFormat.of().formatHex(k1).equals(vectors().get("seed_hex").asText()));
        assertEquals(32, k1.length);
    }
}
