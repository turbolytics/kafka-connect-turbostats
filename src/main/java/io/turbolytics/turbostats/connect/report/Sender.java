package io.turbolytics.turbostats.connect.report;

import io.turbolytics.turbostats.connect.sign.Credential;
import io.turbolytics.turbostats.connect.sign.Signer;
import io.turbolytics.turbostats.connect.wire.Bundle;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/**
 * Posts one signed bundle. The response's commands are ignored in v1.
 *
 * The reporter depends on Port, not on this class, so a test can stand in
 * for the network; pass sender::send where a Port is wanted.
 */
public final class Sender {
    /**
     * The receiver's answer. reason is its refusal text, such as "no such
     * key", null on success; retryAfterSeconds is its Retry-After, 0 when
     * it set none.
     */
    public record Answer(int status, String reason, long retryAfterSeconds) {
        public static Answer of(int status) {
            return new Answer(status, null, 0);
        }
    }

    @FunctionalInterface
    public interface Port {
        CompletableFuture<Answer> send(Bundle b, Instant now);
    }

    /** A refusal's text is a line; a body longer than this is not one. */
    static final int REASON_BYTES = 512;
    static final int REASON_CHARS = 200;

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

    public CompletableFuture<Answer> send(Bundle b, Instant now) {
        byte[] body = b.toJson().getBytes(StandardCharsets.UTF_8);
        String path = Signer.requestPath(reportTo.getRawPath());
        HttpRequest.Builder req = HttpRequest.newBuilder(reportTo)
                .timeout(timeout)
                .header("Content-Type", Signer.MEDIA_TYPE)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        Signer.headers(credential, "POST", path, now.getEpochSecond(), body).forEach(req::header);
        // A success's body is discarded: a v1 reporter acts on no command.
        // A refusal's first bytes are kept, because they say why: a 401
        // alone cannot tell a revoked key from a skewed clock.
        // The request timeout stops covering the exchange once headers
        // arrive; a receiver that then stalls the body would hold this post,
        // and with it every later interval, forever. orTimeout bounds it all.
        return client.sendAsync(req.build(), info -> info.statusCode() / 100 == 2
                        ? HttpResponse.BodySubscribers.<String>replacing(null)
                        : new Capped(REASON_BYTES))
                .thenApply(r -> new Answer(r.statusCode(), reason(r.body()), retryAfter(r.headers())))
                .orTimeout(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /** One line of printable text, or null. A receiver's body goes into the worker's log. */
    static String reason(String body) {
        if (body == null) {
            return null;
        }
        String line = body.replaceAll("\\p{Cntrl}", " ").replaceAll("\\s+", " ").strip();
        if (line.isEmpty()) {
            return null;
        }
        return line.length() > REASON_CHARS ? line.substring(0, REASON_CHARS) + "…" : line;
    }

    /** Retry-After in seconds. The HTTP-date form, which Control never sends, reads as none. */
    static long retryAfter(HttpHeaders headers) {
        try {
            return headers.firstValue("Retry-After").map(v -> Math.max(0, Long.parseLong(v.strip()))).orElse(0L);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /** Keeps at most limit bytes of a body, then stops reading it. */
    static final class Capped implements HttpResponse.BodySubscriber<String> {
        private final int limit;
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private final CompletableFuture<String> body = new CompletableFuture<>();
        private Flow.Subscription subscription;

        Capped(int limit) {
            this.limit = limit;
        }

        @Override
        public CompletionStage<String> getBody() {
            return body;
        }

        @Override
        public void onSubscribe(Flow.Subscription s) {
            subscription = s;
            s.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            for (ByteBuffer b : items) {
                int n = Math.min(b.remaining(), limit - out.size());
                byte[] chunk = new byte[n];
                b.get(chunk);
                out.write(chunk, 0, n);
            }
            if (out.size() >= limit) {
                subscription.cancel();
                finish();
            } else {
                subscription.request(1);
            }
        }

        @Override
        public void onError(Throwable t) {
            finish();
        }

        @Override
        public void onComplete() {
            finish();
        }

        private void finish() {
            body.complete(out.toString(StandardCharsets.UTF_8));
        }
    }
}
