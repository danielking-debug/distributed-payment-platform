package com.daniel.distributed.observability;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistryAssert;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentTracingTest {

    @Test
    void outboxPublish_recordsSpanWithPaymentIdAndTopic() {
        TestObservationRegistry registry = TestObservationRegistry.create();
        UUID paymentId = UUID.randomUUID();
        AtomicBoolean ran = new AtomicBoolean(false);

        PaymentTracing.outboxPublish(registry, paymentId, "payments.submitted")
            .observe(() -> ran.set(true));

        assertThat(ran).isTrue();
        TestObservationRegistryAssert.assertThat(registry)
            .hasObservationWithNameEqualTo(PaymentTracing.OUTBOX_PUBLISH)
            .that()
            .hasHighCardinalityKeyValue(PaymentTracing.PAYMENT_ID_ATTRIBUTE, paymentId.toString())
            .hasLowCardinalityKeyValue("messaging.destination", "payments.submitted");
    }

    @Test
    void paymentProcess_recordsSpanWithPaymentId() {
        TestObservationRegistry registry = TestObservationRegistry.create();
        UUID paymentId = UUID.randomUUID();

        PaymentTracing.paymentProcess(registry, paymentId).observe(() -> {
        });

        TestObservationRegistryAssert.assertThat(registry)
            .hasObservationWithNameEqualTo(PaymentTracing.PAYMENT_PROCESS)
            .that()
            .hasHighCardinalityKeyValue(PaymentTracing.PAYMENT_ID_ATTRIBUTE, paymentId.toString());
    }

    @Test
    void dltReplay_recordsSpanWithPaymentIdAndDestination() {
        TestObservationRegistry registry = TestObservationRegistry.create();
        UUID paymentId = UUID.randomUUID();

        PaymentTracing.dltReplay(registry, paymentId, "payments.submitted").observe(() -> {
        });

        TestObservationRegistryAssert.assertThat(registry)
            .hasObservationWithNameEqualTo(PaymentTracing.DLT_REPLAY)
            .that()
            .hasHighCardinalityKeyValue(PaymentTracing.PAYMENT_ID_ATTRIBUTE, paymentId.toString())
            .hasLowCardinalityKeyValue("messaging.destination", "payments.submitted");
    }

    @Test
    void noopRegistry_doesNotThrow() {
        UUID paymentId = UUID.randomUUID();
        AtomicBoolean ran = new AtomicBoolean(false);
        PaymentTracing.paymentProcess(ObservationRegistry.NOOP, paymentId)
            .observe(() -> ran.set(true));
        assertThat(ran).isTrue();
    }
}
