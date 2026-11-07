package com.daniel.distributed.observability;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

import java.util.UUID;

/**
 * Shared observation helpers for payment-related spans (Milestone 6 Phase 2).
 *
 * <p>Architecture (Option A — two correlated traces, no outbox schema change):
 * <ul>
 *   <li>HTTP {@code POST /payments} is one trace (Spring MVC observation).</li>
 *   <li>Outbox publish runs later on a scheduler thread and starts a <strong>new</strong>
 *       root span/trace; Kafka producer/consumer then propagate that second trace via
 *       W3C {@code traceparent} headers (Spring Kafka observation).</li>
 *   <li>Operators correlate the two traces using the high-cardinality attribute
 *       {@value #PAYMENT_ID_ATTRIBUTE} (never as a metric label).</li>
 * </ul>
 */
public final class PaymentTracing {

    /** Span attribute key for correlating HTTP / outbox / consumer work. */
    public static final String PAYMENT_ID_ATTRIBUTE = "payment.id";

    public static final String OUTBOX_PUBLISH = "outbox.publish";
    public static final String PAYMENT_PROCESS = "payment.process";
    public static final String DLT_REPLAY = "dlt.replay";

    private PaymentTracing() {
    }

    public static Observation outboxPublish(ObservationRegistry registry, UUID paymentId, String topic) {
        Observation observation = Observation.createNotStarted(OUTBOX_PUBLISH, registry);
        if (paymentId != null) {
            observation.highCardinalityKeyValue(PAYMENT_ID_ATTRIBUTE, paymentId.toString());
        }
        if (topic != null && !topic.isBlank()) {
            observation.lowCardinalityKeyValue("messaging.destination", topic);
        }
        return observation;
    }

    public static Observation paymentProcess(ObservationRegistry registry, UUID paymentId) {
        Observation observation = Observation.createNotStarted(PAYMENT_PROCESS, registry);
        if (paymentId != null) {
            observation.highCardinalityKeyValue(PAYMENT_ID_ATTRIBUTE, paymentId.toString());
        }
        return observation;
    }

    public static Observation dltReplay(ObservationRegistry registry, UUID paymentId, String mainTopic) {
        Observation observation = Observation.createNotStarted(DLT_REPLAY, registry);
        if (paymentId != null) {
            observation.highCardinalityKeyValue(PAYMENT_ID_ATTRIBUTE, paymentId.toString());
        }
        if (mainTopic != null && !mainTopic.isBlank()) {
            observation.lowCardinalityKeyValue("messaging.destination", mainTopic);
        }
        return observation;
    }
}
