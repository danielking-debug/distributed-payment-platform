package com.daniel.distributed.domain;

import com.daniel.distributed.exception.IllegalPaymentStateTransitionException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaymentStateTransitionTest {

    private Payment paymentWithStatus(PaymentStatus status) {
        return Payment.builder()
            .id(UUID.randomUUID())
            .fromAccountId(UUID.randomUUID())
            .toAccountId(UUID.randomUUID())
            .amount(new BigDecimal("10.0000"))
            .currency("USD")
            .status(status)
            .build();
    }

    @Test
    void pending_to_processing_isAllowed() {
        Payment payment = paymentWithStatus(PaymentStatus.PENDING);

        payment.transitionTo(PaymentStatus.PROCESSING);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.PROCESSING);
    }

    @Test
    void processing_to_completed_isAllowed() {
        Payment payment = paymentWithStatus(PaymentStatus.PROCESSING);

        payment.transitionTo(PaymentStatus.COMPLETED);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
    }

    @Test
    void processing_to_failed_isAllowed() {
        Payment payment = paymentWithStatus(PaymentStatus.PROCESSING);

        payment.transitionTo(PaymentStatus.FAILED);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void pending_to_completed_isIllegal() {
        Payment payment = paymentWithStatus(PaymentStatus.PENDING);

        assertThatThrownBy(() -> payment.transitionTo(PaymentStatus.COMPLETED))
            .isInstanceOf(IllegalPaymentStateTransitionException.class)
            .hasMessageContaining("PENDING")
            .hasMessageContaining("COMPLETED");

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.PENDING);
    }

    @Test
    void completed_to_pending_isIllegal() {
        Payment payment = paymentWithStatus(PaymentStatus.COMPLETED);

        assertThatThrownBy(() -> payment.transitionTo(PaymentStatus.PENDING))
            .isInstanceOf(IllegalPaymentStateTransitionException.class)
            .hasMessageContaining("COMPLETED")
            .hasMessageContaining("PENDING");

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
    }

    @Test
    void failed_to_processing_isIllegal() {
        Payment payment = paymentWithStatus(PaymentStatus.FAILED);

        assertThatThrownBy(() -> payment.transitionTo(PaymentStatus.PROCESSING))
            .isInstanceOf(IllegalPaymentStateTransitionException.class)
            .hasMessageContaining("FAILED")
            .hasMessageContaining("PROCESSING");

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void completed_to_failed_isIllegal() {
        Payment payment = paymentWithStatus(PaymentStatus.COMPLETED);

        assertThatThrownBy(() -> payment.transitionTo(PaymentStatus.FAILED))
            .isInstanceOf(IllegalPaymentStateTransitionException.class);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.COMPLETED);
    }

    @Test
    void pending_to_failed_isIllegal() {
        Payment payment = paymentWithStatus(PaymentStatus.PENDING);

        assertThatThrownBy(() -> payment.transitionTo(PaymentStatus.FAILED))
            .isInstanceOf(IllegalPaymentStateTransitionException.class);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.PENDING);
    }
}
