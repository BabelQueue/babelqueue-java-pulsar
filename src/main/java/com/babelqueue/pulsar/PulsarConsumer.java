package com.babelqueue.pulsar;

import com.babelqueue.BabelQueueException;
import com.babelqueue.DeadLetters;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.JsonValues;
import com.babelqueue.PoisonRecord;
import com.babelqueue.UnknownUrnException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;

/**
 * Receives from a Pulsar subscription, validates each message, routes it to the handler
 * registered for its URN, and acknowledges it on success. Per §5.2/§5.7 the routing key is
 * the {@code bq-job} message property (the body's {@code job}/{@code urn} is consulted only
 * when the property is absent or blank, i.e. a message produced outside BabelQueue). A
 * throwing handler {@code negativeAcknowledge}s the message — Pulsar redelivers it
 * (at-least-once) and increments the redelivery count — but only up to {@link Builder#maxAttempts(int) maxAttempts}
 * (default 3, ADR-0036): see "Max attempts and poison" below. {@code attempts} comes from the
 * authoritative {@code bq-attempts} property (valid only if it matches {@code ^[0-9]+$} and fits an
 * {@code int}; otherwise the body's value is used) and is reconciled to
 * {@code max(that, getRedeliveryCount())} for the handler. When {@code bq-job} and the body's
 * {@code job} differ, the property still routes and the optional {@code onRoutingMismatch} hook
 * is notified (a throwing hook is swallowed; the message is settled normally). The loop never stops on a bad
 * message — observe via {@code onError}/{@code onUnknownUrn}.
 *
 * <p>Per §5.7 step 3 the {@code bq-schema-version} property is checked <b>before</b> the body is
 * decoded: when it is present and is not exactly the schema version the core supports
 * ({@link EnvelopeCodec#SCHEMA_VERSION}, compared exactly as sent, as its canonical decimal string, with no
 * trimming — so {@code "2"}, {@code "x"}, {@code "01"} and {@code " 1"} are all unknown), the body is never decoded or routed; {@code onError}
 * is notified (with a {@code null} envelope) and the message is settled on its <b>first</b> delivery
 * (ADR-0037 decision 1; outside the max-attempts bound below, no retry): with a {@link Builder#deadLetterProducer
 * dead-letter producer} the body is written to the DLQ exactly as received (no {@code dead_letter} block) and then
 * acknowledged — a failed write {@code negativeAcknowledge}s it, so the write is retried; without one it is
 * acknowledged and the drop is logged. The gate reads no strategy ({@code onUnknownUrn} never affects it). A missing or blank (empty or ASCII-whitespace-only: space, tab, LF, VT, FF, CR) property changes nothing, and a
 * {@code "1"} property still goes through the post-decode {@link EnvelopeCodec#accepts} check.
 *
 * <h2>Max attempts and poison (ADR-0036, broker-bindings §5.5)</h2>
 *
 * <p>Pulsar has no broker-side redelivery limit and this consumer configures no native
 * {@code DeadLetterPolicy}, so the bound is SDK-owned. Every delivery is <em>counted</em>:
 * {@code counted = reconciled attempts + 1} (the reconciled value is the one described above). A retryable
 * outcome — a handler that throws, an unmapped URN without an {@code onUnknownUrn} hook, or a poison body — is
 * settled by comparing {@code counted} with {@code maxAttempts}:
 * <ul>
 *   <li>{@code counted < maxAttempts}: <b>retry</b> — a re-publish to the same topic with {@code bq-attempts = counted}
 *       and an acknowledgement when a {@link Builder#retryProducer retry producer} or a {@link Builder#client client}
 *       (the consumer then creates the producer itself) is configured, which works on every subscription type;
 *       otherwise a {@code negativeAcknowledge} (the redelivery count advances). {@link Builder#build()} only accepts
 *       that last form for a declared {@code Shared} or {@code Key_Shared} {@link Builder#subscriptionType
 *       subscription}, because Pulsar does not track redeliveries for {@code Exclusive} and {@code Failover};</li>
 *   <li>otherwise, with a {@link Builder#deadLetterProducer dead-letter producer} configured: <b>dlq</b> — the
 *       envelope, annotated with a {@code dead_letter} block ({@code reason} {@code failed} or {@code unknown_urn},
 *       {@code dead_letter.attempts = counted}), is sent to the producer's topic ({@code <queue>.dlq}) and the
 *       original is acknowledged <b>only after</b> that write succeeded (a failed write negative-acknowledges it, so
 *       it is redelivered and written again);</li>
 *   <li>otherwise: <b>drop</b> — the message is acknowledged and the drop is logged.</li>
 * </ul>
 *
 * <p>A <em>poison</em> body — not valid UTF-8, not a JSON object, or an object that is not a conformant envelope
 * with a valid {@code job}/{@code urn} identity — is never handed to a handler. With a dead-letter producer it is
 * written to the DLQ at once, without retry, in the ADR-0035 shape ({@code reason: "poison"}, the original body
 * preserved), then acknowledged; without one it takes the bounded retry above and is finally dropped.
 */
public final class PulsarConsumer {

    private static final System.Logger LOGGER = System.getLogger(PulsarConsumer.class.getName());

    /** Default total number of deliveries a message gets, the first included (ADR-0036 decision 1). */
    public static final int DEFAULT_MAX_ATTEMPTS = 3;

    private static final String JOB_PROPERTY = "bq-job";
    private static final String ATTEMPTS_PROPERTY = "bq-attempts";
    private static final String SCHEMA_VERSION_PROPERTY = "bq-schema-version";

    /**
     * Notified of a non-conformant envelope, an unmapped URN (no {@code onUnknownUrn}), a throwing
     * handler, a transient receive error, or a message rejected by the §5.7 step 3 schema-version
     * gate. {@code envelope} is {@code null} when the body was not decoded (a version-gated message
     * or a receive error) — guard against it before dereferencing. {@code message} is {@code null}
     * for a receive error.
     */
    @FunctionalInterface
    public interface ErrorHandler {
        void onError(Throwable error, Envelope envelope, Message<byte[]> message);
    }

    /** Called instead of erroring when a URN has no handler; the message is then acknowledged. */
    @FunctionalInterface
    public interface UnknownUrnHandler {
        void onUnknownUrn(Envelope envelope, Message<byte[]> message);
    }

    /** Optional advisory hook: {@code bq-job} (routed on) differs from the body's {@code job}. Never affects settling. */
    @FunctionalInterface
    public interface RoutingMismatchHandler {
        void onRoutingMismatch(String propertyUrn, String bodyUrn, Envelope envelope, Message<byte[]> message);
    }

    private final Consumer<byte[]> consumer;
    private final Map<String, BabelHandler> handlers;
    private final int receiveTimeoutMillis;
    private final ErrorHandler onError;
    private final UnknownUrnHandler onUnknownUrn;
    private final RoutingMismatchHandler onRoutingMismatch;
    private final int maxAttempts;
    private final Producer<byte[]> deadLetterProducer;
    private final Producer<byte[]> retryProducer;
    private final PulsarClient client;
    /** Retry producers this consumer created from {@code client}, by topic; closed by {@link #close()}. */
    private final Map<String, Producer<byte[]>> ownedRetryProducers = new ConcurrentHashMap<>();

    private PulsarConsumer(Builder builder) {
        this.consumer = builder.consumer;
        this.handlers = Map.copyOf(builder.handlers);
        this.receiveTimeoutMillis = builder.receiveTimeoutMillis;
        this.onError = builder.onError;
        this.onUnknownUrn = builder.onUnknownUrn;
        this.onRoutingMismatch = builder.onRoutingMismatch;
        this.maxAttempts = builder.maxAttempts;
        this.deadLetterProducer = builder.deadLetterProducer;
        this.retryProducer = builder.retryProducer;
        this.client = builder.client;
    }

    public static Builder builder(Consumer<byte[]> consumer) {
        return new Builder(consumer);
    }

    /** Receive one message (up to the receive timeout), route + settle it. Returns 1 if handled, else 0. */
    public int poll() throws PulsarClientException {
        Message<byte[]> message = consumer.receive(receiveTimeoutMillis, TimeUnit.MILLISECONDS);
        if (message == null) {
            return 0;
        }
        handle(message);
        return 1;
    }

    /** Poll until the current thread is interrupted. */
    public void run() {
        run(() -> !Thread.currentThread().isInterrupted());
    }

    /** Poll while {@code shouldContinue} returns true; a transient receive error is reported and retried. */
    public void run(BooleanSupplier shouldContinue) {
        while (shouldContinue.getAsBoolean()) {
            try {
                poll();
            } catch (PulsarClientException error) {
                report(error, null, null);
            }
        }
    }

    private void handle(Message<byte[]> message) throws PulsarClientException {
        Map<String, String> properties = message.getProperties();

        // §5.7 step 3: version-gate on the property before decoding the body.
        // The value is compared exactly as sent (no trimming), like the Go/Python/Node consumers (GR-5).
        String declaredVersion = rawProperty(properties, SCHEMA_VERSION_PROPERTY);
        if (declaredVersion != null && !isBlankSchemaVersion(declaredVersion)
            && !declaredVersion.equals(Integer.toString(EnvelopeCodec.SCHEMA_VERSION))) {
            settleVersionGated(message, declaredVersion);
            return;
        }

        byte[] body = message.getValue() == null ? new byte[0] : message.getValue();
        String attemptsProperty = rawProperty(properties, ATTEMPTS_PROPERTY);
        int redeliveryCount = message.getRedeliveryCount();

        String text = strictUtf8(body);
        Envelope decoded = text == null ? null : EnvelopeCodec.decode(text);
        if (decoded == null || !EnvelopeCodec.accepts(decoded)) {
            // Poison (ADR-0036 decision 2/4): never handed to a handler. A poison body has no decodable
            // attempts of its own, so the count comes from bq-attempts and the redelivery count only.
            routePoison(message, decoded, body, reconciledAttempts(0, attemptsProperty, redeliveryCount));
            return;
        }
        int attempts = reconciledAttempts(decoded.attempts(), attemptsProperty, redeliveryCount);
        Envelope envelope = attempts == decoded.attempts() ? decoded : decoded.withAttempts(attempts);

        // §5.2 / §5.7 step 2: route on bq-job; fall back to the body only when it is absent.
        String routed = property(properties, JOB_PROPERTY);
        String bodyUrn = EnvelopeCodec.urn(envelope);
        String urn = routed != null ? routed : bodyUrn;
        if (routed != null && !routed.equals(bodyUrn)) {
            // The property wins (§5.7), but the handler sees the body's job: tell the optional
            // onRoutingMismatch hook. A throwing hook is swallowed — settling continues unchanged.
            notifyRoutingMismatch(routed, bodyUrn, envelope, message);
        }
        BabelHandler handler = handlers.get(urn);
        if (handler == null) {
            if (onUnknownUrn != null) {
                onUnknownUrn.onUnknownUrn(envelope, message);
                consumer.acknowledge(message);
            } else {
                retryOrExhaust(message, envelope, new UnknownUrnException(urn), "unknown_urn");
            }
            return;
        }

        try {
            handler.handle(envelope, message);
        } catch (Exception error) {
            retryOrExhaust(message, envelope, error, "failed");
            return;
        }
        consumer.acknowledge(message);
    }

    /**
     * Settles a retryable outcome of a decoded envelope (ADR-0036 decision 1): retry while
     * {@code counted < maxAttempts}, then dead-letter (a producer is configured) or drop.
     */
    private void retryOrExhaust(Message<byte[]> message, Envelope envelope, Throwable cause, String reason)
            throws PulsarClientException {
        report(cause, envelope, message);
        long counted = (long) envelope.attempts() + 1;
        if (counted < maxAttempts) {
            retry(message, envelope, null, counted);
            return;
        }
        if (deadLetterProducer == null) {
            drop(message, reason, counted, cause);
            return;
        }
        byte[] record;
        Map<String, String> properties;
        try {
            Envelope annotated = DeadLetters.annotate(
                envelope.withAttempts((int) Math.min(counted, Integer.MAX_VALUE)),
                reason,
                originalQueue(message, envelope),
                (int) Math.min(counted, Integer.MAX_VALUE),
                cause.getMessage(),
                cause.getClass().getName());
            record = EnvelopeCodec.encode(annotated).getBytes(StandardCharsets.UTF_8);
            properties = new LinkedHashMap<>(PulsarProperties.of(annotated));
            // Keep the route the message arrived on (bq-job wins over the body's job, §5.7), as the retry path does.
            String routed = property(message.getProperties(), JOB_PROPERTY);
            if (routed != null) {
                properties.put(JOB_PROPERTY, routed);
            }
        } catch (RuntimeException error) {
            leaveUnsettled(message, envelope, error);
            return;
        }
        writeDeadLetterAndAcknowledge(message, envelope, record, properties);
    }

    /**
     * Settles a version-gated delivery on its first delivery (ADR-0037 decision 1, broker-bindings §5.7): the body is
     * never decoded, no handler or {@code onUnknownUrn} hook runs and nothing is retried, whatever {@code bq-attempts},
     * the redelivery count or {@code maxAttempts} say. This consumer has no unknown-URN strategy option, so the
     * configured strategy is {@code fail}: with a {@link Builder#deadLetterProducer dead-letter producer} the body is
     * written to the DLQ <b>exactly as received</b> (no {@code dead_letter} block, no re-encoding) and the original is
     * acknowledged only after the write succeeded (a failed write negative-acknowledges it, so the gate rejects the
     * redelivery again and the write is retried); without one the delivery is acknowledged and the drop is logged.
     */
    private void settleVersionGated(Message<byte[]> message, String declaredVersion) throws PulsarClientException {
        BabelQueueException cause = new BabelQueueException("Rejected a Pulsar message: unsupported "
            + SCHEMA_VERSION_PROPERTY + " property '" + declaredVersion + "' (supported: "
            + EnvelopeCodec.SCHEMA_VERSION + "); body not decoded.");
        report(cause, null, message);
        if (deadLetterProducer == null) {
            LOGGER.log(System.Logger.Level.WARNING,
                "BabelQueue Pulsar: dropped a version-gated message from {0} (no dead-letter producer configured): {1}",
                queueName(message), cause.getMessage());
            consumer.acknowledge(message);
            return;
        }
        byte[] body = message.getValue() == null ? new byte[0] : message.getValue();
        Map<String, String> original = message.getProperties();
        writeDeadLetterAndAcknowledge(message, null, body, original == null ? Map.of() : original);
    }

    /**
     * Routes a poison body (ADR-0036 decision 2): with a dead-letter producer it is written to the DLQ in the
     * ADR-0035 shape without any retry; without one it takes the bounded retry, then is dropped.
     */
    private void routePoison(Message<byte[]> message, Envelope partial, byte[] body, int reconciled)
            throws PulsarClientException {
        BabelQueueException cause = new BabelQueueException(
            "Rejected a non-conformant BabelQueue envelope from Pulsar.");
        report(cause, partial, message);
        if (deadLetterProducer == null) {
            long counted = (long) reconciled + 1;
            if (counted < maxAttempts) {
                retry(message, null, body, counted);
            } else {
                drop(message, "poison", counted, cause);
            }
            return;
        }
        byte[] record;
        try {
            Map<String, Object> block = new LinkedHashMap<>();
            block.put("reason", "poison");
            block.put("error", cause.getMessage());
            block.put("exception", cause.getClass().getName());
            block.put("failed_at", System.currentTimeMillis());
            block.put("original_queue", queueName(message));
            block.put("attempts", reconciled);
            block.put("lang", EnvelopeCodec.SOURCE_LANG);
            record = PoisonRecord.build(body, JsonValues.encode(block)).getBytes(StandardCharsets.UTF_8);
        } catch (RuntimeException error) {
            leaveUnsettled(message, partial, error);
            return;
        }
        Map<String, String> original = message.getProperties();
        writeDeadLetterAndAcknowledge(message, partial, record, original == null ? Map.of() : original);
    }

    /** Sends {@code record} to the DLQ; the original is acknowledged only after the write succeeded. */
    private void writeDeadLetterAndAcknowledge(
            Message<byte[]> message, Envelope envelope, byte[] record, Map<String, String> properties)
            throws PulsarClientException {
        try {
            send(deadLetterProducer, record, properties);
        } catch (PulsarClientException | RuntimeException error) {
            leaveUnsettled(message, envelope, error);
            return;
        }
        consumer.acknowledge(message);
    }

    /**
     * Retries a delivery. With a {@link Builder#retryProducer retry producer} the message is re-published to the
     * same topic with {@code bq-attempts = counted} (and the body's {@code attempts} in sync for an envelope; a poison
     * body is re-published byte for byte) and the original is acknowledged only after that write succeeded, so the
     * bound does not depend on the broker's redelivery count (which Pulsar does not track for Exclusive and Failover
     * subscriptions). Without one the message is negatively acknowledged and the redelivery count must advance.
     */
    private void retry(Message<byte[]> message, Envelope envelope, byte[] rawBody, long counted)
            throws PulsarClientException {
        if (retryProducer == null && client == null) {
            // Shared / Key_Shared only (checked in build()): the broker advances the redelivery count itself.
            consumer.negativeAcknowledge(message);
            return;
        }
        try {
            Producer<byte[]> producer = retryProducer != null ? retryProducer : ownedRetryProducer(message);
            byte[] value = envelope == null
                ? rawBody
                : EnvelopeCodec.encode(envelope.withAttempts((int) counted)).getBytes(StandardCharsets.UTF_8);
            Map<String, String> properties = new LinkedHashMap<>();
            if (message.getProperties() != null) {
                properties.putAll(message.getProperties());
            }
            properties.put(ATTEMPTS_PROPERTY, Long.toString(counted));
            send(producer, value, properties);
        } catch (PulsarClientException | RuntimeException error) {
            leaveUnsettled(message, envelope, error);
            return;
        }
        consumer.acknowledge(message);
    }

    /** The retry producer this consumer created from its client for the message's topic (cached per topic). */
    private Producer<byte[]> ownedRetryProducer(Message<byte[]> message) throws PulsarClientException {
        String topic = message.getTopicName();
        if (topic == null || topic.isBlank()) {
            topic = consumer.getTopic();
        }
        // Publish to the partitioned topic itself, not to the one partition the message came from.
        String target = topic.replaceFirst("-partition-[0-9]+$", "");
        Producer<byte[]> existing = ownedRetryProducers.get(target);
        if (existing != null) {
            return existing;
        }
        Producer<byte[]> created = client.newProducer().topic(target).create();
        Producer<byte[]> raced = ownedRetryProducers.putIfAbsent(target, created);
        if (raced == null) {
            return created;
        }
        created.close();
        return raced;
    }

    /**
     * Closes the retry producers this consumer created from its {@link Builder#client client}. A producer passed to
     * {@link Builder#retryProducer} or {@link Builder#deadLetterProducer} belongs to the caller and is left open, and
     * so is the consumer itself.
     */
    public void close() throws PulsarClientException {
        PulsarClientException failure = null;
        for (Producer<byte[]> producer : ownedRetryProducers.values()) {
            try {
                producer.close();
            } catch (PulsarClientException error) {
                failure = error;
            }
        }
        ownedRetryProducers.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private static void send(Producer<byte[]> producer, byte[] value, Map<String, String> properties)
            throws PulsarClientException {
        TypedMessageBuilder<byte[]> out = producer.newMessage().value(value);
        for (Map.Entry<String, String> property : properties.entrySet()) {
            out = out.property(property.getKey(), property.getValue());
        }
        out.send();
    }

    /** A failed DLQ write never loses the message: report it and hand it back (negative ack) to be redelivered. */
    private void leaveUnsettled(Message<byte[]> message, Envelope envelope, Throwable error)
            throws PulsarClientException {
        report(error, envelope, message);
        consumer.negativeAcknowledge(message);
    }

    /** Retries exhausted and no dead-letter configured: acknowledge so it leaves the topic, and say so. */
    private void drop(Message<byte[]> message, String reason, long counted, Throwable cause)
            throws PulsarClientException {
        LOGGER.log(System.Logger.Level.WARNING,
            "BabelQueue Pulsar: dropped a message from {0} after {1} delivery attempt(s) (max {2}, reason {3}, "
                + "no dead-letter producer configured): {4}",
            queueName(message), counted, maxAttempts, reason, cause.getMessage());
        consumer.acknowledge(message);
    }

    /** The DLQ record's {@code original_queue}: the envelope's {@code meta.queue}, else the consumed topic. */
    private String originalQueue(Message<byte[]> message, Envelope envelope) {
        String queue = envelope.meta() == null ? null : envelope.meta().queue();
        return queue != null && !queue.isBlank() ? queue : queueName(message);
    }

    /** The short queue name of the consumed topic: tenant/namespace and a {@code -partition-N} suffix removed. */
    private String queueName(Message<byte[]> message) {
        String topic = message.getTopicName();
        if (topic == null || topic.isBlank()) {
            topic = consumer.getTopic();
        }
        if (topic == null || topic.isBlank()) {
            return "unknown";
        }
        int slash = topic.lastIndexOf('/');
        String name = slash >= 0 ? topic.substring(slash + 1) : topic;
        return name.replaceFirst("-partition-[0-9]+$", "");
    }

    /** The body as text, or {@code null} when it is not valid UTF-8 (never U+FFFD substitution). */
    private static String strictUtf8(byte[] body) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(body)).toString();
        } catch (CharacterCodingException invalid) {
            return null;
        }
    }

    private void notifyRoutingMismatch(String propertyUrn, String bodyUrn, Envelope envelope, Message<byte[]> message) {
        if (onRoutingMismatch == null) {
            return;
        }
        try {
            onRoutingMismatch.onRoutingMismatch(propertyUrn, bodyUrn, envelope, message);
        } catch (RuntimeException ignored) {
            // The hook is advisory: it must never alter ack/nack or stop the consume loop.
        }
    }

    /** The trimmed property value, or {@code null} when the map or the value is missing/blank. */
    private static String property(Map<String, String> properties, String key) {
        if (properties == null) {
            return null;
        }
        String value = properties.get(key);
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.strip();
    }

    /** The property value exactly as sent (no trimming), or {@code null} when the map or the value is missing. */
    private static String rawProperty(Map<String, String> properties, String key) {
        return properties == null ? null : properties.get(key);
    }

    /**
     * The shared cross-SDK "blank" definition for {@code bq-schema-version}: the empty string, or a value made up
     * <b>only</b> of ASCII whitespace ({@code ' '}, {@code \t}, {@code \n}, U+000B, {@code \f}, {@code \r}). Anything else
     * (NBSP, U+001C–U+001F, U+0085, U+FEFF, …) is <b>not</b> blank, unlike {@link String#isBlank()}.
     */
    private static boolean isBlankSchemaVersion(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c != ' ' && (c < '\t' || c > '\r')) {
                return false;
            }
        }
        return true;
    }

    /** {@code ^[0-9]+$}: ASCII digits only — no sign, no whitespace, not empty, no Unicode digits. */
    private static boolean isAsciiDigits(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    /**
     * The reconciled attempts (§5.5): the authoritative {@code bq-attempts} property replaces {@code bodyAttempts},
     * falling back to it when the property is absent or does not match {@code ^[0-9]+$} (or overflows {@code int});
     * the result is then raised to {@code getRedeliveryCount()} if that is higher. Pulsar's redelivery count is
     * 0-based (0 on first delivery) so it maps directly to {@code attempts}; the max never lowers the carried-over
     * count.
     */
    private static int reconciledAttempts(int bodyAttempts, String attemptsProperty, int redeliveryCount) {
        int attempts = bodyAttempts;
        if (isAsciiDigits(attemptsProperty)) {
            try {
                attempts = Integer.parseInt(attemptsProperty);
            } catch (NumberFormatException ignored) {
                // Exceeds the int range — keep the body's value.
            }
        }
        return Math.max(attempts, redeliveryCount);
    }

    private void report(Throwable error, Envelope envelope, Message<byte[]> message) {
        if (onError != null) {
            onError.onError(error, envelope, message);
        }
    }

    /** Fluent builder for {@link PulsarConsumer}. */
    public static final class Builder {
        private final Consumer<byte[]> consumer;
        private final Map<String, BabelHandler> handlers = new HashMap<>();
        private int receiveTimeoutMillis = 1000;
        private ErrorHandler onError;
        private UnknownUrnHandler onUnknownUrn;
        private RoutingMismatchHandler onRoutingMismatch;
        private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
        private Producer<byte[]> deadLetterProducer;
        private Producer<byte[]> retryProducer;
        private PulsarClient client;
        private SubscriptionType subscriptionType;

        private Builder(Consumer<byte[]> consumer) {
            this.consumer = Objects.requireNonNull(consumer, "consumer");
        }

        /** Register {@code handler} for {@code urn} (the last registration wins). */
        public Builder handler(String urn, BabelHandler handler) {
            this.handlers.put(urn, handler);
            return this;
        }

        public Builder handlers(Map<String, BabelHandler> handlers) {
            this.handlers.putAll(handlers);
            return this;
        }

        /** Per-poll receive timeout in milliseconds (default 1000). */
        public Builder receiveTimeout(int millis) {
            this.receiveTimeoutMillis = millis;
            return this;
        }

        public Builder onError(ErrorHandler handler) {
            this.onError = handler;
            return this;
        }

        public Builder onUnknownUrn(UnknownUrnHandler handler) {
            this.onUnknownUrn = handler;
            return this;
        }

        /** Optional hook for a {@code bq-job}/body {@code job} mismatch (default: none). Exceptions it throws are swallowed. */
        public Builder onRoutingMismatch(RoutingMismatchHandler handler) {
            this.onRoutingMismatch = handler;
            return this;
        }

        /**
         * Total number of deliveries a message gets before it is dead-lettered or dropped, the first delivery
         * included (default {@value #DEFAULT_MAX_ATTEMPTS}; {@code 1} means no retry). Pulsar has no broker-side
         * limit and the SDK sets no native {@code DeadLetterPolicy}, so this is the only bound (ADR-0036).
         *
         * @throws IllegalArgumentException when {@code maxAttempts} is below 1
         */
        public Builder maxAttempts(int maxAttempts) {
            if (maxAttempts < 1) {
                throw new IllegalArgumentException("maxAttempts must be at least 1, got " + maxAttempts);
            }
            this.maxAttempts = maxAttempts;
            return this;
        }

        /**
         * Turns dead-lettering on: the producer must publish to the DLQ topic, by convention {@code <queue>.dlq}
         * (error-handling §6). An exhausted message and a poison body are written there as a
         * {@code dead_letter}-annotated record, and the original is acknowledged only after the write succeeded.
         * Without a producer (the default) an exhausted message is acknowledged and dropped with a log line.
         */
        public Builder deadLetterProducer(Producer<byte[]> producer) {
            this.deadLetterProducer = producer;
            return this;
        }

        /**
         * Makes a retry a re-publish instead of a negative acknowledgement: the producer must publish to the topic
         * this consumer reads. The message is re-published with {@code bq-attempts + 1} (the body's {@code attempts}
         * in sync, the other properties kept) and the original is acknowledged after that write succeeded
         * (broker-bindings §5.5, the Go and Python runtimes' model). It counts attempts itself, so the bound works on
         * every subscription type. Takes precedence over {@link #client}; the caller owns the producer.
         */
        public Builder retryProducer(Producer<byte[]> producer) {
            this.retryProducer = producer;
            return this;
        }

        /**
         * Lets the consumer create its own retry producer from {@code client} (one per topic, on the first retry), so
         * the retry is a re-publish as with {@link #retryProducer} without you wiring a producer. Ignored for retries
         * when a {@link #retryProducer} is given. Close the producers with {@link PulsarConsumer#close()}.
         */
        public Builder client(PulsarClient client) {
            this.client = client;
            return this;
        }

        /**
         * Declares the type of the subscription {@code consumer} was created with (the {@code Consumer} interface does
         * not expose it). Without a {@link #retryProducer} or a {@link #client}, a retry is a negative
         * acknowledgement, which only terminates where the broker advances {@code getRedeliveryCount()}: {@code Shared}
         * and {@code Key_Shared}. Pulsar does not track redeliveries for {@code Exclusive} (the client default) and
         * {@code Failover}, so {@link #build()} rejects those, and an undeclared type (assumed to be the client
         * default, {@code Exclusive}), unless a retry producer or a client is given. Declaring it has no other effect.
         */
        public Builder subscriptionType(SubscriptionType subscriptionType) {
            this.subscriptionType = subscriptionType;
            return this;
        }

        /**
         * @throws IllegalStateException when neither a {@link #retryProducer} nor a {@link #client} is given and the
         *         declared {@link #subscriptionType} is not {@code Shared} or {@code Key_Shared} (including when it
         *         is not declared): a negative acknowledgement could never reach {@code maxAttempts} there
         */
        public PulsarConsumer build() {
            boolean republishes = retryProducer != null || client != null;
            boolean broker = subscriptionType == SubscriptionType.Shared || subscriptionType == SubscriptionType.Key_Shared;
            if (!republishes && !broker) {
                throw new IllegalStateException("Retries need a retry producer or a client here: the subscription is "
                    + (subscriptionType == null ? "not declared (the Pulsar client default is Exclusive)" : subscriptionType)
                    + ", and Pulsar does not advance getRedeliveryCount() for Exclusive and Failover subscriptions, so a "
                    + "negative acknowledgement would redeliver forever. Call retryProducer(...) or client(...), or "
                    + "subscriptionType(Shared | Key_Shared) if the consumer really is Shared or Key_Shared.");
            }
            return new PulsarConsumer(this);
        }
    }
}
