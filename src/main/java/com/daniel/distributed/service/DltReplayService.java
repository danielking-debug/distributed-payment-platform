package com.daniel.distributed.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.daniel.distributed.api.dto.DltReplayDtos.ReplayRequest;
import com.daniel.distributed.api.dto.DltReplayDtos.ReplayResponse;
import com.daniel.distributed.config.PaymentProperties;
import com.daniel.distributed.exception.InvalidDltReplayException;
import com.daniel.distributed.kafka.DltSupport;
import com.daniel.distributed.kafka.event.PaymentSubmittedEvent;
import com.daniel.distributed.observability.PaymentLedgerMetrics;
import com.daniel.distributed.observability.PaymentTracing;
import io.micrometer.observation.ObservationRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Controlled <em>manual</em> DLT replay: validates a dead-lettered record and
 * republishes it to the original main topic via {@link KafkaTemplate} only.
 *
 * <p>Does <strong>not</strong> call {@link PaymentProcessingService} directly.
 * The normal consumer path applies idempotency ({@code processed_payment_events},
 * PENDING gate). Does not create payments or change payment IDs.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DltReplayService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final PaymentProperties paymentProperties;
    private final ObjectMapper objectMapper;
    private final ObservationRegistry observationRegistry;
    private final PaymentLedgerMetrics metrics;

    public ReplayResponse replay(ReplayRequest request) {
        final String dltTopic;
        final String mainTopic;
        final String payload;
        final String paymentId;
        final String key;
        try {
            dltTopic = request.dltTopic().trim();
            mainTopic = resolveMainTopic(dltTopic);
            payload = request.payload().trim();
            paymentId = requirePaymentId(payload);
            key = resolveAndValidateKey(mainTopic, request.key(), payload);
        } catch (InvalidDltReplayException e) {
            metrics.dltReplayRejected();
            throw e;
        }

        log.info(
            "DLT manual replay requested: dltTopic={} partition={} offset={} mainTopic={} key={} paymentId={}",
            dltTopic, request.partition(), request.offset(), mainTopic, key, paymentId);

        UUID paymentUuid = parseUuidQuietly(paymentId);
        try {
            ReplayResponse response = PaymentTracing.dltReplay(observationRegistry, paymentUuid, mainTopic)
                .observe(() -> publishReplay(request, dltTopic, mainTopic, key, payload, paymentId));
            metrics.dltReplayPublished();
            return response;
        } catch (InvalidDltReplayException e) {
            // publishReplay only: validation already completed above
            metrics.dltReplayPublishFailed();
            throw e;
        }
    }

    private ReplayResponse publishReplay(
            ReplayRequest request,
            String dltTopic,
            String mainTopic,
            String key,
            String payload,
            String paymentId) {
        try {
            // KafkaTemplate observation injects W3C headers on the republished record.
            kafkaTemplate.send(mainTopic, key, payload).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error(
                "DLT manual replay publish failed: dltTopic={} partition={} offset={} mainTopic={} paymentId={} error={}",
                dltTopic, request.partition(), request.offset(), mainTopic, paymentId, e.toString());
            throw new InvalidDltReplayException(
                "Failed to publish replay to topic '" + mainTopic + "': " + e.getMessage(), e);
        }

        log.info(
            "DLT manual replay published: dltTopic={} partition={} offset={} mainTopic={} key={} paymentId={}",
            dltTopic, request.partition(), request.offset(), mainTopic, key, paymentId);

        return new ReplayResponse(
            dltTopic,
            request.partition(),
            request.offset(),
            mainTopic,
            key,
            paymentId,
            "PUBLISHED",
            "Republished to " + mainTopic + "; processing will follow the normal consumer path");
    }

    /**
     * Maps a known DLT topic to its main topic. Unknown or non-DLT names are rejected.
     */
    String resolveMainTopic(String dltTopic) {
        if (dltTopic == null || dltTopic.isBlank()) {
            throw new InvalidDltReplayException("dltTopic is required");
        }
        Map<String, String> map = Map.of(
            paymentProperties.getSubmittedDltTopic(), paymentProperties.getSubmittedTopic(),
            paymentProperties.getCompletedDltTopic(), paymentProperties.getCompletedTopic(),
            paymentProperties.getFailedDltTopic(), paymentProperties.getFailedTopic()
        );
        String main = map.get(dltTopic);
        if (main == null) {
            throw new InvalidDltReplayException(
                "Unknown or unsupported DLT topic '" + dltTopic
                    + "'. Expected one of: "
                    + paymentProperties.getSubmittedDltTopic() + ", "
                    + paymentProperties.getCompletedDltTopic() + ", "
                    + paymentProperties.getFailedDltTopic());
        }
        return main;
    }

    private String requirePaymentId(String payload) {
        String paymentId = DltSupport.extractPaymentId(payload);
        if (paymentId == null || paymentId.isBlank()) {
            throw new InvalidDltReplayException(
                "Payload is not valid replay JSON: missing or unreadable paymentId");
        }
        // Ensure it is a UUID-shaped id for submitted events we fully parse below
        try {
            java.util.UUID.fromString(paymentId);
        } catch (IllegalArgumentException e) {
            throw new InvalidDltReplayException("paymentId is not a valid UUID: " + paymentId);
        }
        return paymentId;
    }

    /**
     * For payments.submitted, key must be fromAccountId (partition ordering).
     * Key may be omitted if it can be derived from a valid PaymentSubmittedEvent body.
     */
    String resolveAndValidateKey(String mainTopic, String providedKey, String payload) {
        if (mainTopic.equals(paymentProperties.getSubmittedTopic())) {
            PaymentSubmittedEvent event = parseSubmitted(payload);
            String expectedKey = event.fromAccountId().toString();
            if (providedKey == null || providedKey.isBlank()) {
                return expectedKey;
            }
            if (!expectedKey.equals(providedKey.trim())) {
                throw new InvalidDltReplayException(
                    "Kafka key must equal fromAccountId for payments.submitted replay. "
                        + "expected=" + expectedKey + " provided=" + providedKey.trim());
            }
            return expectedKey;
        }

        // completed / failed: require an explicit key (still preserve if operator supplies original)
        if (providedKey == null || providedKey.isBlank()) {
            throw new InvalidDltReplayException(
                "Kafka key is required when replaying topic " + mainTopic);
        }
        // Still require parseable JSON with paymentId (already checked)
        if (!payload.trim().startsWith("{")) {
            throw new InvalidDltReplayException("Payload must be a JSON object");
        }
        try {
            objectMapper.readTree(payload);
        } catch (Exception e) {
            throw new InvalidDltReplayException("Payload is not valid JSON", e);
        }
        return providedKey.trim();
    }


    private static UUID parseUuidQuietly(String value) {
        try {
            return UUID.fromString(value);
        } catch (Exception e) {
            return null;
        }
    }

    private PaymentSubmittedEvent parseSubmitted(String payload) {
        try {
            PaymentSubmittedEvent event = objectMapper.readValue(payload, PaymentSubmittedEvent.class);
            if (event.paymentId() == null || event.fromAccountId() == null
                || event.toAccountId() == null || event.amount() == null
                || event.currency() == null) {
                throw new InvalidDltReplayException(
                    "PaymentSubmittedEvent payload is missing required fields");
            }
            return event;
        } catch (InvalidDltReplayException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidDltReplayException(
                "Payload is not a valid PaymentSubmittedEvent: " + e.getMessage(), e);
        }
    }
}
