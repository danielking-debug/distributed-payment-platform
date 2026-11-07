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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

class ReconciliationServiceTest {

    @Mock private PaymentRepository paymentRepository;
    @Mock private LedgerEntryRepository ledgerEntryRepository;
    @Mock private ProcessedPaymentEventRepository processedPaymentEventRepository;

    private ReconciliationService service;
    private ReconciliationProperties properties;

    private UUID fromId;
    private UUID toId;
    private UUID paymentId;
    private Instant now;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        properties = new ReconciliationProperties();
        properties.setLookbackHours(24);
        properties.setStuckPendingMinutes(30);
        service = new ReconciliationService(
            paymentRepository, ledgerEntryRepository, processedPaymentEventRepository, properties,
            new PaymentLedgerMetrics(new SimpleMeterRegistry()));
        fromId = UUID.randomUUID();
        toId = UUID.randomUUID();
        paymentId = UUID.randomUUID();
        now = Instant.now();
    }

    @Test
    void healthyCompleted_noFindings() {
        Payment p = payment(PaymentStatus.COMPLETED, now.minus(1, ChronoUnit.HOURS));
        List<Finding> findings = service.checkPayment(p, healthyEntries(p), true, stuckCutoff());
        assertThat(findings).isEmpty();
    }

    @Test
    void completedNoLedger_critical() {
        Payment p = payment(PaymentStatus.COMPLETED, now);
        assertHasCode(service.checkPayment(p, List.of(), true, stuckCutoff()), "COMPLETED_MISSING_LEDGER");
    }

    @Test
    void completedOnlyDebit_critical() {
        Payment p = payment(PaymentStatus.COMPLETED, now);
        List<LedgerEntry> entries = List.of(entry(p, EntryType.DEBIT, fromId, p.getAmount()));
        assertHasCode(service.checkPayment(p, entries, true, stuckCutoff()), "COMPLETED_MISSING_CREDIT");
    }

    @Test
    void completedOnlyCredit_critical() {
        Payment p = payment(PaymentStatus.COMPLETED, now);
        List<LedgerEntry> entries = List.of(entry(p, EntryType.CREDIT, toId, p.getAmount()));
        assertHasCode(service.checkPayment(p, entries, true, stuckCutoff()), "COMPLETED_MISSING_DEBIT");
    }

    @Test
    void completedDuplicateDebit_critical() {
        Payment p = payment(PaymentStatus.COMPLETED, now);
        List<LedgerEntry> entries = List.of(
            entry(p, EntryType.DEBIT, fromId, p.getAmount()),
            entry(p, EntryType.DEBIT, fromId, p.getAmount()),
            entry(p, EntryType.CREDIT, toId, p.getAmount()));
        assertHasCode(service.checkPayment(p, entries, true, stuckCutoff()), "COMPLETED_DUPLICATE_DEBIT");
    }

    @Test
    void completedWrongAmount_critical() {
        Payment p = payment(PaymentStatus.COMPLETED, now);
        List<LedgerEntry> entries = List.of(
            entry(p, EntryType.DEBIT, fromId, new BigDecimal("1.00")),
            entry(p, EntryType.CREDIT, toId, new BigDecimal("1.00")));
        List<Finding> findings = service.checkPayment(p, entries, true, stuckCutoff());
        assertThat(codes(findings)).anyMatch(c -> c.contains("AMOUNT"));
    }

    @Test
    void completedWrongAccount_critical() {
        Payment p = payment(PaymentStatus.COMPLETED, now);
        UUID wrong = UUID.randomUUID();
        List<LedgerEntry> entries = List.of(
            entry(p, EntryType.DEBIT, wrong, p.getAmount()),
            entry(p, EntryType.CREDIT, toId, p.getAmount()));
        assertHasCode(service.checkPayment(p, entries, true, stuckCutoff()), "COMPLETED_WRONG_DEBIT_ACCOUNT");
    }

    @Test
    void completedMissingProcessed_critical() {
        Payment p = payment(PaymentStatus.COMPLETED, now);
        assertHasCode(service.checkPayment(p, healthyEntries(p), false, stuckCutoff()),
            "COMPLETED_MISSING_PROCESSED");
    }

    @Test
    void healthyFailed_noFindings() {
        Payment p = payment(PaymentStatus.FAILED, now);
        assertThat(service.checkPayment(p, List.of(), true, stuckCutoff())).isEmpty();
    }

    @Test
    void failedWithLedger_critical() {
        Payment p = payment(PaymentStatus.FAILED, now);
        List<LedgerEntry> entries = healthyEntries(p);
        assertHasCode(service.checkPayment(p, entries, true, stuckCutoff()), "FAILED_HAS_LEDGER");
    }

    @Test
    void pendingYoung_noFinding() {
        Payment p = payment(PaymentStatus.PENDING, now.minus(5, ChronoUnit.MINUTES));
        assertThat(service.checkPayment(p, List.of(), false, stuckCutoff())).isEmpty();
    }

    @Test
    void pendingOlderThanThreshold_stuckWarning() {
        Payment p = payment(PaymentStatus.PENDING, now.minus(2, ChronoUnit.HOURS));
        List<Finding> findings = service.checkPayment(p, List.of(), false, stuckCutoff());
        assertHasCode(findings, "STUCK_PENDING");
        assertThat(findings.get(0).severity()).isEqualTo(ReconciliationService.SEVERITY_WARNING);
    }

    @Test
    void pendingWithProcessed_critical() {
        Payment p = payment(PaymentStatus.PENDING, now.minus(1, ChronoUnit.MINUTES));
        assertHasCode(service.checkPayment(p, List.of(), true, stuckCutoff()), "PENDING_HAS_PROCESSED");
    }

    @Test
    void unexpectedProcessing_anomaly() {
        Payment p = payment(PaymentStatus.PROCESSING, now);
        List<Finding> findings = service.checkPayment(p, List.of(), false, stuckCutoff());
        assertHasCode(findings, "UNEXPECTED_PROCESSING");
        assertThat(findings.get(0).severity()).isEqualTo(ReconciliationService.SEVERITY_ANOMALY);
    }

    @Test
    void runTwiceHealthy_stableNoFindings() {
        Payment p = payment(PaymentStatus.COMPLETED, now.minus(1, ChronoUnit.HOURS));
        when(paymentRepository.findByCreatedAtGreaterThanEqual(any())).thenReturn(List.of(p));
        when(paymentRepository.findByStatusAndCreatedAtLessThanEqual(eq(PaymentStatus.PENDING), any()))
            .thenReturn(List.of());
        when(ledgerEntryRepository.findByPaymentIdIn(any())).thenReturn(healthyEntries(p));
        when(processedPaymentEventRepository.findAllById(any()))
            .thenReturn(List.of(new ProcessedPaymentEvent(p.getId())));

        RunResponse first = service.run(new RunRequest(null, null, null));
        RunResponse second = service.run(new RunRequest(null, null, null));

        assertThat(first.findingCount()).isZero();
        assertThat(second.findingCount()).isZero();
        assertThat(first.paymentsScanned()).isEqualTo(1);
        assertThat(second.paymentsScanned()).isEqualTo(1);
    }

    @Test
    void targetedPaymentId() {
        Payment p = payment(PaymentStatus.COMPLETED, now);
        when(paymentRepository.findById(paymentId)).thenReturn(Optional.of(p));
        when(ledgerEntryRepository.findByPaymentIdIn(any())).thenReturn(List.of());
        when(processedPaymentEventRepository.findAllById(any()))
            .thenReturn(List.of(new ProcessedPaymentEvent(paymentId)));

        RunResponse response = service.run(new RunRequest(null, null, paymentId));

        assertThat(response.paymentsScanned()).isEqualTo(1);
        assertThat(codes(response.findings())).contains("COMPLETED_MISSING_LEDGER");
    }

    @Test
    void bulkLoad_usesFindByPaymentIdInAndFindAllById_notPerPaymentQueries() {
        Payment p1 = payment(PaymentStatus.COMPLETED, now.minus(2, ChronoUnit.HOURS));
        UUID id2 = UUID.randomUUID();
        Payment p2 = Payment.builder()
            .id(id2)
            .fromAccountId(fromId)
            .toAccountId(toId)
            .amount(new BigDecimal("10.0000"))
            .currency("USD")
            .status(PaymentStatus.FAILED)
            .createdAt(now.minus(1, ChronoUnit.HOURS))
            .updatedAt(now.minus(1, ChronoUnit.HOURS))
            .build();

        when(paymentRepository.findByCreatedAtGreaterThanEqual(any())).thenReturn(List.of(p1, p2));
        when(paymentRepository.findByStatusAndCreatedAtLessThanEqual(eq(PaymentStatus.PENDING), any()))
            .thenReturn(List.of());
        when(ledgerEntryRepository.findByPaymentIdIn(any())).thenReturn(healthyEntries(p1));
        when(processedPaymentEventRepository.findAllById(any())).thenReturn(List.of(
            new ProcessedPaymentEvent(p1.getId()),
            new ProcessedPaymentEvent(p2.getId())));

        RunResponse response = service.run(new RunRequest(null, null, null));

        assertThat(response.paymentsScanned()).isEqualTo(2);
        assertThat(response.findingCount()).isZero();
        // Fewer than one chunk → single bulk call each
        verify(ledgerEntryRepository, times(1)).findByPaymentIdIn(any());
        verify(processedPaymentEventRepository, times(1)).findAllById(any());
        verify(ledgerEntryRepository, never()).findByPaymentId(any());
        verify(processedPaymentEventRepository, never()).existsById(any());
    }

    @Test
    void emptyCandidates_skipsBulkQueries() {
        when(paymentRepository.findByCreatedAtGreaterThanEqual(any())).thenReturn(List.of());
        when(paymentRepository.findByStatusAndCreatedAtLessThanEqual(eq(PaymentStatus.PENDING), any()))
            .thenReturn(List.of());

        RunResponse response = service.run(new RunRequest(null, null, null));

        assertThat(response.paymentsScanned()).isZero();
        assertThat(response.findingCount()).isZero();
        verify(ledgerEntryRepository, never()).findByPaymentIdIn(any());
        verify(processedPaymentEventRepository, never()).findAllById(any());
        verify(ledgerEntryRepository, never()).findByPaymentId(any());
        verify(processedPaymentEventRepository, never()).existsById(any());
    }

    @Test
    void partition_emptyAndBoundaries() {
        assertThat(ReconciliationService.partition(List.of(), 1000)).isEmpty();
        assertThat(ReconciliationService.partition(List.of("a"), 1000)).containsExactly(List.of("a"));
        assertThat(ReconciliationService.partition(List.of(1, 2, 3), 2))
            .containsExactly(List.of(1, 2), List.of(3));
        List<Integer> exact = java.util.stream.IntStream.range(0, 1000).boxed().toList();
        assertThat(ReconciliationService.partition(exact, 1000)).containsExactly(exact);
        List<Integer> over = java.util.stream.IntStream.range(0, 1001).boxed().toList();
        List<List<Integer>> parts = ReconciliationService.partition(over, 1000);
        assertThat(parts).hasSize(2);
        assertThat(parts.get(0)).hasSize(1000);
        assertThat(parts.get(1)).hasSize(1);
    }

    @Test
    void bulkLoad_exactlyOneFullChunk_singleRepositoryCall() {
        List<Payment> payments = nPayments(ReconciliationService.BULK_ID_CHUNK_SIZE, PaymentStatus.FAILED);
        when(paymentRepository.findByCreatedAtGreaterThanEqual(any())).thenReturn(payments);
        when(paymentRepository.findByStatusAndCreatedAtLessThanEqual(eq(PaymentStatus.PENDING), any()))
            .thenReturn(List.of());
        when(ledgerEntryRepository.findByPaymentIdIn(any())).thenReturn(List.of());
        when(processedPaymentEventRepository.findAllById(any())).thenAnswer(inv -> {
            @SuppressWarnings("unchecked")
            Iterable<UUID> ids = inv.getArgument(0);
            List<ProcessedPaymentEvent> events = new ArrayList<>();
            ids.forEach(id -> events.add(new ProcessedPaymentEvent(id)));
            return events;
        });

        RunResponse response = service.run(new RunRequest(null, null, null));

        assertThat(response.paymentsScanned()).isEqualTo(ReconciliationService.BULK_ID_CHUNK_SIZE);
        assertThat(response.findingCount()).isZero();
        verify(ledgerEntryRepository, times(1)).findByPaymentIdIn(any());
        verify(processedPaymentEventRepository, times(1)).findAllById(any());

        ArgumentCaptor<java.util.Collection> ledgerIds = ArgumentCaptor.forClass(java.util.Collection.class);
        verify(ledgerEntryRepository).findByPaymentIdIn(ledgerIds.capture());
        assertThat(ledgerIds.getValue()).hasSize(ReconciliationService.BULK_ID_CHUNK_SIZE);
    }

    @Test
    void bulkLoad_multipleChunks_fetchesEachIdExactlyOnce_andMergesFindings() {
        int total = ReconciliationService.BULK_ID_CHUNK_SIZE + 50; // 2 chunks
        List<Payment> payments = nPayments(total, PaymentStatus.COMPLETED);
        // First payment missing ledger → finding; rest healthy
        Payment first = payments.get(0);
        List<LedgerEntry> entriesForRest = new ArrayList<>();
        for (int i = 1; i < total; i++) {
            entriesForRest.addAll(healthyEntries(payments.get(i)));
        }

        when(paymentRepository.findByCreatedAtGreaterThanEqual(any())).thenReturn(payments);
        when(paymentRepository.findByStatusAndCreatedAtLessThanEqual(eq(PaymentStatus.PENDING), any()))
            .thenReturn(List.of());
        when(ledgerEntryRepository.findByPaymentIdIn(any())).thenAnswer(inv -> {
            @SuppressWarnings("unchecked")
            java.util.Collection<UUID> ids = inv.getArgument(0);
            List<LedgerEntry> out = new ArrayList<>();
            for (LedgerEntry e : entriesForRest) {
                if (ids.contains(e.getPaymentId())) {
                    out.add(e);
                }
            }
            return out;
        });
        when(processedPaymentEventRepository.findAllById(any())).thenAnswer(inv -> {
            @SuppressWarnings("unchecked")
            Iterable<UUID> ids = inv.getArgument(0);
            List<ProcessedPaymentEvent> events = new ArrayList<>();
            ids.forEach(id -> events.add(new ProcessedPaymentEvent(id)));
            return events;
        });

        RunResponse response = service.run(new RunRequest(null, null, null));

        assertThat(response.paymentsScanned()).isEqualTo(total);
        // COMPLETED without ledger → at least COMPLETED_MISSING_LEDGER for first
        assertThat(codes(response.findings())).contains("COMPLETED_MISSING_LEDGER");
        assertThat(response.findings().stream().map(Finding::paymentId).toList())
            .contains(first.getId());

        int expectedChunks = 2;
        verify(ledgerEntryRepository, times(expectedChunks)).findByPaymentIdIn(any());
        verify(processedPaymentEventRepository, times(expectedChunks)).findAllById(any());

        ArgumentCaptor<java.util.Collection> ledgerCaptor = ArgumentCaptor.forClass(java.util.Collection.class);
        verify(ledgerEntryRepository, times(expectedChunks)).findByPaymentIdIn(ledgerCaptor.capture());
        List<java.util.Collection> ledgerChunks = ledgerCaptor.getAllValues();
        assertThat(ledgerChunks.get(0)).hasSize(ReconciliationService.BULK_ID_CHUNK_SIZE);
        assertThat(ledgerChunks.get(1)).hasSize(50);
        // Every payment id requested exactly once across chunks
        java.util.Set<UUID> seen = new java.util.HashSet<>();
        for (java.util.Collection<?> chunk : ledgerChunks) {
            for (Object id : chunk) {
                assertThat(seen.add((UUID) id)).isTrue();
            }
        }
        assertThat(seen).hasSize(total);
        assertThat(seen).containsExactlyInAnyOrderElementsOf(
            payments.stream().map(Payment::getId).toList());
    }

    private List<Payment> nPayments(int n, PaymentStatus status) {
        List<Payment> list = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            list.add(Payment.builder()
                .id(UUID.randomUUID())
                .fromAccountId(fromId)
                .toAccountId(toId)
                .amount(new BigDecimal("10.0000"))
                .currency("USD")
                .status(status)
                .createdAt(now.minus(1, ChronoUnit.HOURS))
                .updatedAt(now.minus(1, ChronoUnit.HOURS))
                .build());
        }
        return list;
    }

    private Instant stuckCutoff() {
        return now.minus(properties.getStuckPendingMinutes(), ChronoUnit.MINUTES);
    }

    private Payment payment(PaymentStatus status, Instant createdAt) {
        return Payment.builder()
            .id(paymentId)
            .fromAccountId(fromId)
            .toAccountId(toId)
            .amount(new BigDecimal("50.0000"))
            .currency("USD")
            .status(status)
            .createdAt(createdAt)
            .updatedAt(createdAt)
            .build();
    }

    private List<LedgerEntry> healthyEntries(Payment p) {
        return List.of(
            entry(p, EntryType.DEBIT, fromId, p.getAmount()),
            entry(p, EntryType.CREDIT, toId, p.getAmount()));
    }

    private LedgerEntry entry(Payment p, EntryType type, UUID accountId, BigDecimal amount) {
        return LedgerEntry.builder()
            .id(UUID.randomUUID())
            .accountId(accountId)
            .paymentId(p.getId())
            .entryType(type)
            .amount(amount)
            .balanceAfter(amount)
            .createdAt(now)
            .build();
    }

    private static void assertHasCode(List<Finding> findings, String code) {
        assertThat(codes(findings)).contains(code);
    }

    private static List<String> codes(List<Finding> findings) {
        return findings.stream().map(Finding::code).collect(Collectors.toList());
    }
}
