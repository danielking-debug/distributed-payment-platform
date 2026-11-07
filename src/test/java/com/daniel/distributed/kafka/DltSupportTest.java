package com.daniel.distributed.kafka;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DltSupportTest {

    @Test
    void extractPaymentId_fromValidJson() {
        String json = "{\"paymentId\":\"11111111-1111-1111-1111-111111111111\",\"amount\":10}";
        assertThat(DltSupport.extractPaymentId(json))
            .isEqualTo("11111111-1111-1111-1111-111111111111");
    }

    @Test
    void extractPaymentId_malformedJson_returnsNull() {
        assertThat(DltSupport.extractPaymentId("not-json{{{")).isNull();
    }

    @Test
    void extractPaymentId_missingField_returnsNull() {
        assertThat(DltSupport.extractPaymentId("{\"amount\":1}")).isNull();
    }

    @Test
    void extractPaymentId_nullOrBlank_returnsNull() {
        assertThat(DltSupport.extractPaymentId((String) null)).isNull();
        assertThat(DltSupport.extractPaymentId("  ")).isNull();
    }

    @Test
    void isNonRetryable_jsonProcessingException() {
        JsonProcessingException ex = new JsonParseException(null, "bad");
        assertThat(DltSupport.isNonRetryableDeserialization(ex)).isTrue();
    }

    @Test
    void isNonRetryable_wrappedInRuntimeException() {
        Throwable wrapped = new RuntimeException(
            new JsonMappingException(null, "cannot map"));
        assertThat(DltSupport.isNonRetryableDeserialization(wrapped)).isTrue();
    }

    @Test
    void isNonRetryable_serializationException() {
        assertThat(DltSupport.isNonRetryableDeserialization(new SerializationException("x"))).isTrue();
    }

    @Test
    void isNonRetryable_transientStyleException_false() {
        assertThat(DltSupport.isNonRetryableDeserialization(new RuntimeException("db timeout"))).isFalse();
        assertThat(DltSupport.isNonRetryableDeserialization(new IllegalStateException("lock"))).isFalse();
    }
}
