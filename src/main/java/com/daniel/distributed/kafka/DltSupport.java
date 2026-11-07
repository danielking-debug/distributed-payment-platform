package com.daniel.distributed.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Helpers for dead-letter handling: best-effort payment id extraction from
 * event JSON without failing the DLT path if the payload is unreadable.
 */
public final class DltSupport {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DltSupport() {}

    /**
     * Attempts to read {@code paymentId} from a JSON event body.
     * Returns {@code null} if the value is missing or the payload is not JSON.
     */
    public static String extractPaymentId(String payload) {
        if (payload == null || payload.isBlank()) {
            return null;
        }
        try {
            JsonNode node = MAPPER.readTree(payload);
            JsonNode id = node.get("paymentId");
            if (id == null || id.isNull()) {
                return null;
            }
            return id.asText(null);
        } catch (Exception e) {
            return null;
        }
    }

    public static String extractPaymentId(ConsumerRecord<?, ?> record) {
        if (record == null || record.value() == null) {
            return null;
        }
        return extractPaymentId(record.value().toString());
    }

    /**
     * True if this exception (or any cause) indicates a non-retryable
     * malformed / deserialization failure.
     */
    public static boolean isNonRetryableDeserialization(Throwable thrown) {
        Throwable t = thrown;
        while (t != null) {
            if (t instanceof com.fasterxml.jackson.core.JsonProcessingException) {
                return true;
            }
            // Common wrappers / alternate deserializer failures
            String name = t.getClass().getName();
            if (name.contains("JsonParseException")
                || name.contains("JsonMappingException")
                || name.contains("SerializationException")) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
