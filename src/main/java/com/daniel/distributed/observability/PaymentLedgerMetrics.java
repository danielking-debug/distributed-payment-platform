package com.daniel.distributed.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Low-cardinality domain metrics for the payment ledger (Milestone 6 Phase 3).
 * Tag values are fixed enums only — never payment/account IDs or free-text messages.
 */
@Component
public class PaymentLedgerMetrics {

    public static final String TAG_OUTCOME = "outcome";
    public static final String TAG_SEVERITY = "severity";

    private final MeterRegistry registry;

    public PaymentLedgerMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    // --- payments.submitted ---

    public void paymentSubmittedCreated() {
        counter("payment_ledger.payments.submitted", "created");
    }

    public void paymentSubmittedReplayed() {
        counter("payment_ledger.payments.submitted", "replayed");
    }

    public void paymentSubmittedConflict() {
        counter("payment_ledger.payments.submitted", "conflict");
    }

    public void paymentSubmittedRejected() {
        counter("payment_ledger.payments.submitted", "rejected");
    }

    // --- payments.processed ---

    public void paymentProcessedCompleted(long durationNanos) {
        recordProcessed("completed", durationNanos);
    }

    public void paymentProcessedFailed(long durationNanos) {
        recordProcessed("failed", durationNanos);
    }

    public void paymentProcessedSkippedDuplicate(long durationNanos) {
        recordProcessed("skipped_duplicate", durationNanos);
    }

    public void paymentProcessedSkippedNotPending(long durationNanos) {
        recordProcessed("skipped_not_pending", durationNanos);
    }

    /**
     * Timer only for unexpected failures in processing. Does not increment the
     * business-level {@code payments.processed} counter.
     */
    public void paymentProcessedError(long durationNanos) {
        Timer.builder("payment_ledger.payments.processing")
            .tag(TAG_OUTCOME, "error")
            .publishPercentileHistogram()
            .register(registry)
            .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    // --- outbox ---

    public void outboxPublishSuccess() {
        counter("payment_ledger.outbox.publish", "success");
    }

    public void outboxPublishFailure() {
        counter("payment_ledger.outbox.publish", "failure");
    }

    // --- reconciliation ---

    public void reconciliationRunSuccess() {
        counter("payment_ledger.reconciliation.runs", "success");
    }

    public void reconciliationDuration(long durationNanos) {
        Timer.builder("payment_ledger.reconciliation")
            .publishPercentileHistogram()
            .register(registry)
            .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    private static final java.util.Set<String> RECON_SEVERITIES = java.util.Set.of(
        "CRITICAL", "WARNING", "ANOMALY");

    /**
     * Increments findings only for the closed severity set. Unknown values are
     * ignored so they cannot create unbounded Prometheus series.
     */
    public void reconciliationFinding(String severity) {
        if (severity == null || !RECON_SEVERITIES.contains(severity)) {
            return;
        }
        registry.counter("payment_ledger.reconciliation.findings", TAG_SEVERITY, severity).increment();
    }

    // --- DLT manual replay ---

    public void dltReplayPublished() {
        counter("payment_ledger.dlt.replay", "published");
    }

    public void dltReplayRejected() {
        counter("payment_ledger.dlt.replay", "rejected");
    }

    public void dltReplayPublishFailed() {
        counter("payment_ledger.dlt.replay", "publish_failed");
    }

    public <T> T timeReconciliation(Supplier<T> work) {
        long start = System.nanoTime();
        try {
            return work.get();
        } finally {
            reconciliationDuration(System.nanoTime() - start);
        }
    }

    private void recordProcessed(String outcome, long durationNanos) {
        counter("payment_ledger.payments.processed", outcome);
        Timer.builder("payment_ledger.payments.processing")
            .tag(TAG_OUTCOME, outcome)
            .publishPercentileHistogram()
            .register(registry)
            .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    private void counter(String name, String outcome) {
        registry.counter(name, TAG_OUTCOME, outcome).increment();
    }
}
