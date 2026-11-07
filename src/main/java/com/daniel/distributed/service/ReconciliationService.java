package com.daniel.distributed.service;

import com.daniel.distributed.api.dto.ReconciliationDtos.Finding;
import com.daniel.distributed.api.dto.ReconciliationDtos.RunRequest;
import com.daniel.distributed.api.dto.ReconciliationDtos.RunResponse;
import com.daniel.distributed.config.ReconciliationProperties;
import com.daniel.distributed.domain.EntryType;
import com.daniel.distributed.domain.LedgerEntry;
import com.daniel.distributed.domain.Payment;
import com.daniel.distributed.domain.PaymentStatus;
import com.daniel.distributed.domain.ProcessedPaymentEvent;
import com.daniel.distributed.repository.LedgerEntryRepository;
import com.daniel.distributed.repository.PaymentRepository;
import com.daniel.distributed.observability.PaymentLedgerMetrics;
import com.daniel.distributed.repository.ProcessedPaymentEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Read-only payment/ledger reconciliation. Detects inconsistencies between
 * payment status, ledger entries, and {@code processed_payment_events}.
 * Does not repair data, publish Kafka events, or transition payment state.
 *
 * <p>Does not attempt to reconstruct account balances from the ledger: account
 * creation does not write opening-balance ledger rows in the current schema.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ReconciliationService {

    public static final String SEVERITY_CRITICAL = "CRITICAL";
    public static final String SEVERITY_WARNING = "WARNING";
    public static final String SEVERITY_ANOMALY = "ANOMALY";

    /**
     * Max IDs per {@code IN (...)} / {@code findAllById} call. PostgreSQL limits
     * prepared-statement parameters to 65,535; well below that keeps plans
     * modest and leaves headroom for other binds. Production lookbacks can
     * exceed 65k payments, so candidates are loaded in chunks of this size.
     */
    static final int BULK_ID_CHUNK_SIZE = 1_000;

    private final PaymentRepository paymentRepository;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final ProcessedPaymentEventRepository processedPaymentEventRepository;
    private final ReconciliationProperties properties;
    private final PaymentLedgerMetrics metrics;

    @Transactional(readOnly = true)
    public RunResponse run(RunRequest request) {
        return metrics.timeReconciliation(() -> runInternal(request));
    }

    private RunResponse runInternal(RunRequest request) {
        long lookbackHours = request != null && request.lookbackHours() != null
            ? request.lookbackHours() : properties.getLookbackHours();
        long stuckMinutes = request != null && request.stuckPendingMinutes() != null
            ? request.stuckPendingMinutes() : properties.getStuckPendingMinutes();

        Instant now = Instant.now();
        Instant lookbackStart = now.minus(lookbackHours, ChronoUnit.HOURS);
        Instant stuckCutoff = now.minus(stuckMinutes, ChronoUnit.MINUTES);

        List<Payment> candidates = selectCandidates(request, lookbackStart, stuckCutoff);
        List<Finding> findings = new ArrayList<>();

        if (!candidates.isEmpty()) {
            List<UUID> paymentIds = candidates.stream().map(Payment::getId).toList();
            Map<UUID, List<LedgerEntry>> entriesByPaymentId = loadLedgerEntriesByPaymentId(paymentIds);
            Set<UUID> processedIds = loadProcessedPaymentIds(paymentIds);

            for (Payment payment : candidates) {
                List<LedgerEntry> entries = entriesByPaymentId.getOrDefault(
                    payment.getId(), Collections.emptyList());
                boolean processed = processedIds.contains(payment.getId());
                findings.addAll(checkPayment(payment, entries, processed, stuckCutoff));
            }
        }

        for (Finding f : findings) {
            if (SEVERITY_CRITICAL.equals(f.severity()) || SEVERITY_ANOMALY.equals(f.severity())) {
                log.warn("Reconciliation finding: paymentId={} status={} code={} severity={} detail={}",
                    f.paymentId(), f.paymentStatus(), f.code(), f.severity(), f.detail());
            } else {
                log.info("Reconciliation finding: paymentId={} status={} code={} severity={} detail={}",
                    f.paymentId(), f.paymentStatus(), f.code(), f.severity(), f.detail());
            }
        }
        log.info("Reconciliation complete: scanned={} findings={} lookbackHours={} stuckPendingMinutes={}",
            candidates.size(), findings.size(), lookbackHours, stuckMinutes);

        for (Finding f : findings) {
            if (f.severity() != null) {
                metrics.reconciliationFinding(f.severity());
            }
        }
        metrics.reconciliationRunSuccess();
        return new RunResponse(now, lookbackHours, stuckMinutes, candidates.size(), findings.size(), findings);
    }


    /**
     * Bulk-load ledger rows for the given payment IDs, chunked so each query
     * stays under PostgreSQL's prepared-statement parameter limit.
     */
    Map<UUID, List<LedgerEntry>> loadLedgerEntriesByPaymentId(List<UUID> paymentIds) {
        Map<UUID, List<LedgerEntry>> entriesByPaymentId = new HashMap<>();
        if (paymentIds == null || paymentIds.isEmpty()) {
            return entriesByPaymentId;
        }
        for (List<UUID> chunk : partition(paymentIds, BULK_ID_CHUNK_SIZE)) {
            for (LedgerEntry entry : ledgerEntryRepository.findByPaymentIdIn(chunk)) {
                entriesByPaymentId
                    .computeIfAbsent(entry.getPaymentId(), id -> new ArrayList<>())
                    .add(entry);
            }
        }
        return entriesByPaymentId;
    }

    /**
     * Bulk-load processed-event presence for payment IDs (entity PK is payment_id).
     */
    Set<UUID> loadProcessedPaymentIds(List<UUID> paymentIds) {
        Set<UUID> processedIds = new HashSet<>();
        if (paymentIds == null || paymentIds.isEmpty()) {
            return processedIds;
        }
        for (List<UUID> chunk : partition(paymentIds, BULK_ID_CHUNK_SIZE)) {
            for (ProcessedPaymentEvent event : processedPaymentEventRepository.findAllById(chunk)) {
                processedIds.add(event.getPaymentId());
            }
        }
        return processedIds;
    }

    /**
     * Split {@code items} into consecutive sublists of at most {@code chunkSize}.
     * Visible for tests that assert query batching.
     */
    static <T> List<List<T>> partition(List<T> items, int chunkSize) {
        if (items == null || items.isEmpty()) {
            return List.of();
        }
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be positive");
        }
        List<List<T>> chunks = new ArrayList<>();
        for (int i = 0; i < items.size(); i += chunkSize) {
            chunks.add(List.copyOf(items.subList(i, Math.min(i + chunkSize, items.size()))));
        }
        return chunks;
    }

    private List<Payment> selectCandidates(RunRequest request, Instant lookbackStart, Instant stuckCutoff) {
        if (request != null && request.paymentId() != null) {
            return paymentRepository.findById(request.paymentId()).stream().toList();
        }
        Map<UUID, Payment> byId = new LinkedHashMap<>();
        for (Payment p : paymentRepository.findByCreatedAtGreaterThanEqual(lookbackStart)) {
            byId.put(p.getId(), p);
        }
        for (Payment p : paymentRepository.findByStatusAndCreatedAtLessThanEqual(
                PaymentStatus.PENDING, stuckCutoff)) {
            byId.putIfAbsent(p.getId(), p);
        }
        return new ArrayList<>(byId.values());
    }

    /**
     * Pure rules against one payment + its ledger rows + processed flag.
     * Visible for focused unit tests with fixtures.
     */
    List<Finding> checkPayment(
            Payment payment,
            List<LedgerEntry> entries,
            boolean processed,
            Instant stuckCutoff) {

        List<Finding> findings = new ArrayList<>();
        PaymentStatus status = payment.getStatus();
        UUID id = payment.getId();
        String statusName = status != null ? status.name() : "null";

        if (status == PaymentStatus.COMPLETED) {
            checkCompleted(payment, entries, processed, findings);
        } else if (status == PaymentStatus.FAILED) {
            checkFailed(payment, entries, processed, findings);
        } else if (status == PaymentStatus.PENDING) {
            checkPending(payment, entries, processed, stuckCutoff, findings);
        } else if (status == PaymentStatus.PROCESSING) {
            findings.add(finding(payment, "UNEXPECTED_PROCESSING", SEVERITY_ANOMALY,
                "PROCESSING is not durable in the current design; found persisted PROCESSING payment"));
            if (!entries.isEmpty()) {
                findings.add(finding(payment, "PROCESSING_HAS_LEDGER", SEVERITY_CRITICAL,
                    "PROCESSING payment has " + entries.size() + " ledger entr(y/ies); money movement unexpected"));
            }
        } else {
            findings.add(finding(payment, "UNKNOWN_STATUS", SEVERITY_ANOMALY,
                "Unrecognized payment status: " + statusName));
        }

        return findings;
    }

    private void checkCompleted(
            Payment payment, List<LedgerEntry> entries, boolean processed, List<Finding> findings) {
        if (!processed) {
            findings.add(finding(payment, "COMPLETED_MISSING_PROCESSED", SEVERITY_CRITICAL,
                "COMPLETED payment has no processed_payment_events row"));
        }

        List<LedgerEntry> debits = entries.stream().filter(e -> e.getEntryType() == EntryType.DEBIT).toList();
        List<LedgerEntry> credits = entries.stream().filter(e -> e.getEntryType() == EntryType.CREDIT).toList();

        if (entries.isEmpty()) {
            findings.add(finding(payment, "COMPLETED_MISSING_LEDGER", SEVERITY_CRITICAL,
                "COMPLETED payment has no ledger entries"));
            return;
        }

        if (debits.isEmpty()) {
            findings.add(finding(payment, "COMPLETED_MISSING_DEBIT", SEVERITY_CRITICAL,
                "COMPLETED payment has no DEBIT ledger entry"));
        } else if (debits.size() > 1) {
            findings.add(finding(payment, "COMPLETED_DUPLICATE_DEBIT", SEVERITY_CRITICAL,
                "COMPLETED payment has " + debits.size() + " DEBIT entries; expected exactly 1"));
        }

        if (credits.isEmpty()) {
            findings.add(finding(payment, "COMPLETED_MISSING_CREDIT", SEVERITY_CRITICAL,
                "COMPLETED payment has no CREDIT ledger entry"));
        } else if (credits.size() > 1) {
            findings.add(finding(payment, "COMPLETED_DUPLICATE_CREDIT", SEVERITY_CRITICAL,
                "COMPLETED payment has " + credits.size() + " CREDIT entries; expected exactly 1"));
        }

        if (debits.size() == 1) {
            LedgerEntry debit = debits.get(0);
            if (!payment.getFromAccountId().equals(debit.getAccountId())) {
                findings.add(finding(payment, "COMPLETED_WRONG_DEBIT_ACCOUNT", SEVERITY_CRITICAL,
                    "DEBIT account " + debit.getAccountId() + " != fromAccountId " + payment.getFromAccountId()));
            }
            if (debit.getAmount().compareTo(payment.getAmount()) != 0) {
                findings.add(finding(payment, "COMPLETED_WRONG_DEBIT_AMOUNT", SEVERITY_CRITICAL,
                    "DEBIT amount " + debit.getAmount() + " != payment amount " + payment.getAmount()));
            }
        }

        if (credits.size() == 1) {
            LedgerEntry credit = credits.get(0);
            if (!payment.getToAccountId().equals(credit.getAccountId())) {
                findings.add(finding(payment, "COMPLETED_WRONG_CREDIT_ACCOUNT", SEVERITY_CRITICAL,
                    "CREDIT account " + credit.getAccountId() + " != toAccountId " + payment.getToAccountId()));
            }
            if (credit.getAmount().compareTo(payment.getAmount()) != 0) {
                findings.add(finding(payment, "COMPLETED_WRONG_CREDIT_AMOUNT", SEVERITY_CRITICAL,
                    "CREDIT amount " + credit.getAmount() + " != payment amount " + payment.getAmount()));
            }
        }

        if (debits.size() == 1 && credits.size() == 1) {
            BigDecimal d = debits.get(0).getAmount();
            BigDecimal c = credits.get(0).getAmount();
            if (d.compareTo(c) != 0) {
                findings.add(finding(payment, "COMPLETED_DEBIT_CREDIT_IMBALANCE", SEVERITY_CRITICAL,
                    "DEBIT amount " + d + " does not equal CREDIT amount " + c));
            }
        }

        long other = entries.stream()
            .filter(e -> e.getEntryType() != EntryType.DEBIT && e.getEntryType() != EntryType.CREDIT)
            .count();
        if (other > 0) {
            findings.add(finding(payment, "COMPLETED_UNEXPECTED_ENTRY_TYPE", SEVERITY_CRITICAL,
                "COMPLETED payment has " + other + " ledger entr(y/ies) with unexpected type"));
        }
    }

    private void checkFailed(
            Payment payment, List<LedgerEntry> entries, boolean processed, List<Finding> findings) {
        if (!processed) {
            findings.add(finding(payment, "FAILED_MISSING_PROCESSED", SEVERITY_CRITICAL,
                "FAILED payment has no processed_payment_events row"));
        }
        if (!entries.isEmpty()) {
            findings.add(finding(payment, "FAILED_HAS_LEDGER", SEVERITY_CRITICAL,
                "FAILED payment has " + entries.size() + " ledger entr(y/ies); expected zero money movement"));
        }
    }

    private void checkPending(
            Payment payment,
            List<LedgerEntry> entries,
            boolean processed,
            Instant stuckCutoff,
            List<Finding> findings) {
        if (processed) {
            findings.add(finding(payment, "PENDING_HAS_PROCESSED", SEVERITY_CRITICAL,
                "PENDING payment has a processed_payment_events row"));
        }
        if (!entries.isEmpty()) {
            findings.add(finding(payment, "PENDING_HAS_LEDGER", SEVERITY_CRITICAL,
                "PENDING payment has " + entries.size() + " ledger entr(y/ies); expected none"));
        }
        Instant created = payment.getCreatedAt();
        if (created != null && !created.isAfter(stuckCutoff)) {
            findings.add(finding(payment, "STUCK_PENDING", SEVERITY_WARNING,
                "PENDING payment older than stuck threshold (createdAt=" + created + ")"));
        }
    }

    private static Finding finding(Payment payment, String code, String severity, String detail) {
        return new Finding(
            payment.getId(),
            payment.getStatus() != null ? payment.getStatus().name() : null,
            code,
            severity,
            detail);
    }
}
