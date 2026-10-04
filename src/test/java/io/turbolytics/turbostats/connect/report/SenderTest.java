package io.turbolytics.turbostats.connect.report;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import io.turbolytics.turbostats.connect.sign.Credential;
import io.turbolytics.turbostats.connect.sign.Signer;
import io.turbolytics.turbostats.connect.wire.Fixtures;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class SenderTest {
    static final String KEY = "sfc_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

    static PublicKey publicKey(byte[] raw) throws Exception {
        byte[] prefix = HexFormat.of().parseHex("302a300506032b6570032100");
        byte[] der = new byte[prefix.length + raw.length];
        System.arraycopy(prefix, 0, der, 0, prefix.length);
        System.arraycopy(raw, 0, der, prefix.length, raw.length);
        return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(der));
    }

    // The receiver verifies what arrives with the public key alone, as a
    // control plane does.
    @Test
    void postsASignedBundleTheReceiverCanVerify() throws Exception {
        Map<String, String> got = new ConcurrentHashMap<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/turbostats", ex -> {
            got.put("body", new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            got.put("type", ex.getRequestHeaders().getFirst("Content-Type"));
            got.put("ts", ex.getRequestHeaders().getFirst(Signer.HEADER_TIMESTAMP));
            got.put("sig", ex.getRequestHeaders().getFirst(Signer.HEADER_SIGNATURE));
            got.put("kid", ex.getRequestHeaders().getFirst(Signer.HEADER_KEY_ID));
            byte[] reply = "{\"v\":1,\"commands\":[]}".getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, reply.length);
            ex.getResponseBody().write(reply);
            ex.close();
        });
        server.start();
        try {
            Credential c = Credential.parse(KEY);
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/v1/turbostats");
            int status = new Sender(uri, c, Duration.ofSeconds(5))
                    .send(Fixtures.sourceBundle(), Instant.ofEpochSecond(1789848000)).get(10, TimeUnit.SECONDS);
            assertEquals(200, status);
            assertEquals(Signer.MEDIA_TYPE, got.get("type"));
            assertEquals(c.keyId(), got.get("kid"));
            assertEquals("1789848000", got.get("ts"));

            Signature v = Signature.getInstance("Ed25519");
            v.initVerify(publicKey(c.publicKey()));
            v.update(Signer.canonical("POST", "/v1/turbostats", 1789848000,
                    got.get("body").getBytes(StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8));
            assertTrue(v.verify(Base64.getDecoder().decode(got.get("sig"))));
        } finally {
            server.stop(0);
        }
    }

    // A receiver that sends headers and then stalls the body would hold the
    // post open forever, and the reporter skips every interval while one is
    // in flight. The timeout covers the whole exchange.
    @Test
    void aStalledBodyTimesOut() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/turbostats", ex -> {
            ex.getRequestBody().readAllBytes();
            ex.sendResponseHeaders(200, 100);
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException ignored) {
            }
            ex.close();
        });
        server.start();
        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/v1/turbostats");
            long t0 = System.nanoTime();
            ExecutionException e = org.junit.jupiter.api.Assertions.assertThrows(ExecutionException.class,
                    () -> new Sender(uri, Credential.parse(KEY), Duration.ofSeconds(1))
                            .send(Fixtures.sourceBundle(), Instant.now()).get(5, TimeUnit.SECONDS));
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 4000, "took too long: " + e);
        } finally {
            server.stop(0);
        }
    }
}
