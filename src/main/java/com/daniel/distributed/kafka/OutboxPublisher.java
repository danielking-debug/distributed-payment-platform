package com.daniel.distributed.kafka;

import com.daniel.distributed.config.OutboxProperties;
import com.daniel.distributed.domain.OutboxEvent;
import com.daniel.distributed.observability.PaymentLedgerMetrics;
import com.daniel.distributed.observability.PaymentTracing;
import com.daniel.distributed.repository.OutboxEventRepository;
import io.micrometer.observation.ObservationRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Polls the {@code outbox_events} table and publishes unpublished rows to
 * Kafka. This is the second half of the transactional outbox pattern: the
 * write side ({@code PaymentWriteService}, {@code PaymentProcessingService})
 * only ever writes to Postgres, inside the same transaction as the domain
 * change; this publisher is the only thing that talks to Kafka, and does so
 * outside any application transaction.
 *
 * <p>Delivery semantics are at-least-once: a row is marked published only
 * after the broker acknowledges it, but if the process crashes between the
 * broker ack and the DB commit that records {@code published_at}, the same
 * row is republished on restart. Consumers must therefore be idempotent
 * (see {@code PaymentProcessingService}), which they are.
 *
 * <p>Tracing (M6 Phase 2, Option A): each publish starts a <strong>new</strong>
 * root observation ({@code outbox.publish}) because the HTTP request that
 * wrote the outbox row has already completed. Spring Kafka template observation
 * injects W3C trace context into producer headers so the consumer continues
 * this second trace. Correlate with the HTTP trace via {@code payment.id}
 * (the outbox {@code aggregate_id}).
 *
 * <p>On Kafka send failure the {@code outbox.publish} observation is marked
 * with the error (via rethrow into {@link io.micrometer.observation.Observation#observe}),
 * while the batch loop catches that exception so later events are still attempted.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxProperties outboxProperties;
    private final ObservationRegistry observationRegistry;
    private final PaymentLedgerMetrics metrics;

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:100}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> batch = outboxEventRepository.findUnpublishedBatchForUpdate(outboxProperties.getBatchSize());
        if (batch.isEmpty()) {
            return;
        }

        for (OutboxEvent event : batch) {
            try {
                // observe() records the error if publishOne throws; the catch keeps
                // batch isolation so one failure does not skip the remaining events.
                PaymentTracing.outboxPublish(observationRegistry, event.getAggregateId(), event.getTopic())
                    .observe(() -> publishOne(event));
            } catch (RuntimeException ex) {
                // Already logged in publishOne; continue with the next outbox row.
            }
        }
    }

    /**
     * Publish a single outbox row. On failure logs, leaves {@code published_at}
     * null, and throws so the enclosing observation is marked erroneous.
     */
    void publishOne(OutboxEvent event) {
        try {
            // KafkaTemplate observation (spring.kafka.template.observation-enabled)
            // creates the producer span and injects W3C headers.
            kafkaTemplate.send(event.getTopic(), event.getPartitionKey(), event.getPayload())
                .get(5, TimeUnit.SECONDS);
            event.setPublishedAt(Instant.now());
            outboxEventRepository.save(event);
            metrics.outboxPublishSuccess();
        } catch (Exception e) {
            // Leave published_at null; this row will be retried on the next poll.
            log.error("Failed to publish outbox event {} to topic {} (will retry)",
                event.getId(), event.getTopic(), e);
            metrics.outboxPublishFailure();
            if (e instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new RuntimeException(e);
        }
    }
}
