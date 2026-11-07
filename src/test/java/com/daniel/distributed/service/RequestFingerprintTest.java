package com.daniel.distributed.service;

import com.daniel.distributed.api.dto.PaymentDtos.CreatePaymentRequest;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class RequestFingerprintTest {

    @Test
    void sameFields_produceSameHash() {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        BigDecimal amount = new BigDecimal("25.5000");
        String a = RequestFingerprint.of(from, to, amount, "USD");
        String b = RequestFingerprint.of(from, to, amount, "USD");
        assertThat(a).isEqualTo(b).hasSize(64);
    }

    @Test
    void differentAmount_producesDifferentHash() {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        String a = RequestFingerprint.of(from, to, new BigDecimal("10.00"), "USD");
        String b = RequestFingerprint.of(from, to, new BigDecimal("10.01"), "USD");
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void currencyNormalizedToUpperCase() {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        BigDecimal amount = new BigDecimal("1.00");
        assertThat(RequestFingerprint.of(from, to, amount, "usd"))
            .isEqualTo(RequestFingerprint.of(from, to, amount, "USD"));
    }

    @Test
    void fromRequestDto_matchesFieldOverload() {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        CreatePaymentRequest req = new CreatePaymentRequest(from, to, new BigDecimal("9.99"), "EUR");
        assertThat(RequestFingerprint.of(req))
            .isEqualTo(RequestFingerprint.of(from, to, new BigDecimal("9.99"), "EUR"));
    }
}
