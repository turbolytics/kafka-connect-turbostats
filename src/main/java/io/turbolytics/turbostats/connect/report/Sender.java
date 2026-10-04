package io.turbolytics.turbostats.connect.report;

import io.turbolytics.turbostats.connect.sign.Credential;
import io.turbolytics.turbostats.connect.sign.Signer;
import io.turbolytics.turbostats.connect.wire.Bundle;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Posts one signed bundle. The response's commands are ignored in v1.
 *
 * The reporter depends on Port, not on this class, so a test can stand in
 * for the network; pass sender::send where a Port is wanted.
 */
public final class Sender {
    @FunctionalInterface
    public interface Port {
        CompletableFuture<Integer> send(Bundle b, Instant now);
    }

    private final URI reportTo;
    private final Credential credential;
    private final Duration timeout;
    private final HttpClient client;

    public Sender(URI reportTo, Credential credential, Duration timeout) {
        this.reportTo = reportTo;
        this.credential = credential;
        this.timeout = timeout;
        this.client = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    public CompletableFuture<Integer> send(Bundle b, Instant now) {
        byte[] body = b.toJson().getBytes(StandardCharsets.UTF_8);
        String path = Signer.requestPath(reportTo.getRawPath());
        HttpRequest.Builder req = HttpRequest.newBuilder(reportTo)
                .timeout(timeout)
                .header("Content-Type", Signer.MEDIA_TYPE)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        Signer.headers(credential, "POST", path, now.getEpochSecond(), body).forEach(req::header);
        // The body is discarded: a v1 reporter acts on no command, and
        // reading it would only hold the connection longer.
        return client.sendAsync(req.build(), HttpResponse.BodyHandlers.discarding()).thenApply(HttpResponse::statusCode);
    }
}
