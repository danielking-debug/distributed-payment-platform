package com.daniel.distributed.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaymentLedgerMetricsTest {

    private MeterRegistry registry;
    private PaymentLedgerMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new PaymentLedgerMetrics(registry);
    }

    @Test
    void submissionOutcomes_areDistinctCounters() {
        metrics.paymentSubmittedCreated();
        metrics.paymentSubmittedReplayed();
        metrics.paymentSubmittedConflict();
        metrics.paymentSubmittedRejected();

        assertThat(counter("payment_ledger.payments.submitted", "created")).isEqualTo(1.0);
        assertThat(counter("payment_ledger.payments.submitted", "replayed")).isEqualTo(1.0);
        assertThat(counter("payment_ledger.payments.submitted", "conflict")).isEqualTo(1.0);
        assertThat(counter("payment_ledger.payments.submitted", "rejected")).isEqualTo(1.0);
    }

    @Test
    void processingRecordsCounterAndTimer() {
        metrics.paymentProcessedCompleted(1_000_000L);
        assertThat(counter("payment_ledger.payments.processed", "completed")).isEqualTo(1.0);
        assertThat(registry.find("payment_ledger.payments.processing")
            .tag("outcome", "completed").timer()).isNotNull();
        assertThat(registry.find("payment_ledger.payments.processing")
            .tag("outcome", "completed").timer().count()).isEqualTo(1);
    }

    @Test
    void outboxPublishOutcomes() {
        metrics.outboxPublishSuccess();
        metrics.outboxPublishFailure();
        assertThat(counter("payment_ledger.outbox.publish", "success")).isEqualTo(1.0);
        assertThat(counter("payment_ledger.outbox.publish", "failure")).isEqualTo(1.0);
    }

    @Test
    void reconciliationDurationAlways_successOnlyOnComplete() {
        metrics.timeReconciliation(() -> "ok");
        assertThat(registry.find("payment_ledger.reconciliation").timer().count()).isEqualTo(1);

        assertThatThrownBy(() -> metrics.timeReconciliation(() -> {
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(registry.find("payment_ledger.reconciliation").timer().count()).isEqualTo(2);

        metrics.reconciliationRunSuccess();
        assertThat(counter("payment_ledger.reconciliation.runs", "success")).isEqualTo(1.0);
    }

    @Test
    void dltReplayOutcomes() {
        metrics.dltReplayPublished();
        metrics.dltReplayRejected();
        metrics.dltReplayPublishFailed();
        assertThat(counter("payment_ledger.dlt.replay", "published")).isEqualTo(1.0);
        assertThat(counter("payment_ledger.dlt.replay", "rejected")).isEqualTo(1.0);
        assertThat(counter("payment_ledger.dlt.replay", "publish_failed")).isEqualTo(1.0);
    }

    private double counter(String name, String outcome) {
        return registry.find(name).tag("outcome", outcome).counter().count();
    }

    @Test
    void unsupportedSeverity_doesNotCreateMetricSeries() {
        metrics.reconciliationFinding("CRITICAL");
        metrics.reconciliationFinding("NOT_A_REAL_SEVERITY");
        metrics.reconciliationFinding(null);

        assertThat(registry.find("payment_ledger.reconciliation.findings")
            .tag("severity", "CRITICAL").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("payment_ledger.reconciliation.findings")
            .tag("severity", "NOT_A_REAL_SEVERITY").counter()).isNull();
    }

    @Test
    void paymentProcessedError_timerOnly_noBusinessFailedCounter() {
        metrics.paymentProcessedError(500_000L);
        assertThat(registry.find("payment_ledger.payments.processing")
            .tag("outcome", "error").timer().count()).isEqualTo(1);
        assertThat(registry.find("payment_ledger.payments.processed")
            .tag("outcome", "failed").counter()).isNull();
        assertThat(registry.find("payment_ledger.payments.processed")
            .tag("outcome", "error").counter()).isNull();
    }
}
