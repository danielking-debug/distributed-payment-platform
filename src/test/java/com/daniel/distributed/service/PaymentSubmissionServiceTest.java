package com.daniel.distributed.service;

import com.daniel.distributed.api.dto.PaymentDtos.CreatePaymentRequest;
import com.daniel.distributed.api.dto.PaymentDtos.PaymentResponse;
import com.daniel.distributed.domain.Payment;
import com.daniel.distributed.domain.PaymentStatus;
import com.daniel.distributed.exception.IdempotencyConflictException;
import com.daniel.distributed.exception.InvalidIdempotencyKeyException;
import com.daniel.distributed.repository.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import com.daniel.distributed.observability.PaymentLedgerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.mockito.MockitoAnnotations;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class PaymentSubmissionServiceTest {

    @Mock private PaymentRepository paymentRepository;
    @Mock private PaymentWriteService paymentWriteService;
    @Mock private IdempotencyService idempotencyService;

    private PaymentSubmissionService service;
    private PaymentLedgerMetrics metrics;
    private SimpleMeterRegistry meterRegistry;

    private UUID fromId;
    private UUID toId;
    private CreatePaymentRequest request;
    private String fingerprint;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        meterRegistry = new SimpleMeterRegistry();
        metrics = new PaymentLedgerMetrics(meterRegistry);
        service = new PaymentSubmissionService(paymentRepository, paymentWriteService, idempotencyService, metrics);
        fromId = UUID.randomUUID();
        toId = UUID.randomUUID();
        request = new CreatePaymentRequest(fromId, toId, new BigDecimal("50.0000"), "USD");
        fingerprint = RequestFingerprint.of(request);
        when(idempotencyService.getCachedEntry(any())).thenReturn(Optional.empty());
    }

    @Test
    void blankKey_rejected() {
        assertThatThrownBy(() -> service.submit(request, "   "))
            .isInstanceOf(InvalidIdempotencyKeyException.class);
        verifyNoInteractions(paymentWriteService);
    }

    @Test
    void nullKey_rejected() {
        assertThatThrownBy(() -> service.submit(request, null))
            .isInstanceOf(InvalidIdempotencyKeyException.class);
    }

    @Test
    void overlongKey_rejected() {
        assertThatThrownBy(() -> service.submit(request, "k".repeat(256)))
            .isInstanceOf(InvalidIdempotencyKeyException.class)
            .hasMessageContaining("255");
    }

    @Test
    void replayWhilePending_returnsPending() {
        Payment existing = paymentWithStatus(PaymentStatus.PENDING, fingerprint);
        when(paymentRepository.findByIdempotencyKey("key-1")).thenReturn(Optional.of(existing));

        PaymentResponse response = service.submit(request, "key-1");

        assertThat(response.status()).isEqualTo("PENDING");
        assertThat(response.id()).isEqualTo(existing.getId());
        verify(paymentWriteService, never()).createPaymentAndEnqueue(any(), any(), any());
        verify(idempotencyService).cachePaymentIndex(eq("key-1"), eq(fingerprint), eq(existing.getId()));
    }

    @Test
    void replayAfterCompleted_returnsCompleted() {
        Payment existing = paymentWithStatus(PaymentStatus.COMPLETED, fingerprint);
        when(paymentRepository.findByIdempotencyKey("key-1")).thenReturn(Optional.of(existing));

        PaymentResponse response = service.submit(request, "key-1");

        assertThat(response.status()).isEqualTo("COMPLETED");
        assertThat(response.id()).isEqualTo(existing.getId());
        verify(paymentWriteService, never()).createPaymentAndEnqueue(any(), any(), any());
    }

    @Test
    void replayAfterFailed_returnsFailed() {
        Payment existing = paymentWithStatus(PaymentStatus.FAILED, fingerprint);
        existing.setFailureReason("INSUFFICIENT_FUNDS");
        when(paymentRepository.findByIdempotencyKey("key-1")).thenReturn(Optional.of(existing));

        PaymentResponse response = service.submit(request, "key-1");

        assertThat(response.status()).isEqualTo("FAILED");
        assertThat(response.failureReason()).isEqualTo("INSUFFICIENT_FUNDS");
        verify(paymentWriteService, never()).createPaymentAndEnqueue(any(), any(), any());
    }

    @Test
    void redisWarmAndCold_sameCurrentStatus_completed() {
        Payment existing = paymentWithStatus(PaymentStatus.COMPLETED, fingerprint);

        // Warm Redis: index hit → load by id from Postgres
        when(idempotencyService.getCachedEntry("key-1"))
            .thenReturn(Optional.of(new IdempotencyService.CachedEntry(fingerprint, existing.getId())));
        when(paymentRepository.findById(existing.getId())).thenReturn(Optional.of(existing));

        PaymentResponse warm = service.submit(request, "key-1");
        assertThat(warm.status()).isEqualTo("COMPLETED");

        // Cold Redis: miss → load by idempotency key from Postgres
        when(idempotencyService.getCachedEntry("key-1")).thenReturn(Optional.empty());
        when(paymentRepository.findByIdempotencyKey("key-1")).thenReturn(Optional.of(existing));

        PaymentResponse cold = service.submit(request, "key-1");
        assertThat(cold.status()).isEqualTo("COMPLETED");
        assertThat(cold.id()).isEqualTo(warm.id());
        verify(paymentWriteService, never()).createPaymentAndEnqueue(any(), any(), any());
    }

    @Test
    void redisWarm_doesNotReturnStalePending_whenDbIsCompleted() {
        UUID paymentId = UUID.randomUUID();
        // Redis only knows id + hash (no status). DB has COMPLETED.
        when(idempotencyService.getCachedEntry("key-1"))
            .thenReturn(Optional.of(new IdempotencyService.CachedEntry(fingerprint, paymentId)));
        Payment completed = paymentWithStatus(PaymentStatus.COMPLETED, fingerprint);
        completed.setId(paymentId);
        when(paymentRepository.findById(paymentId)).thenReturn(Optional.of(completed));

        PaymentResponse response = service.submit(request, "key-1");

        assertThat(response.status()).isEqualTo("COMPLETED");
        assertThat(response.id()).isEqualTo(paymentId);
    }

    @Test
    void sameKeyDifferentRequest_throwsConflict() {
        String otherHash = RequestFingerprint.of(fromId, toId, new BigDecimal("99.00"), "USD");
        Payment existing = paymentWithStatus(PaymentStatus.PENDING, otherHash);
        when(paymentRepository.findByIdempotencyKey("key-1")).thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> service.submit(request, "key-1"))
            .isInstanceOf(IdempotencyConflictException.class);
        verify(paymentWriteService, never()).createPaymentAndEnqueue(any(), any(), any());
    }

    @Test
    void sameKeyDifferentRequest_fromRedisIndex_throwsConflict() {
        when(idempotencyService.getCachedEntry("key-1"))
            .thenReturn(Optional.of(new IdempotencyService.CachedEntry("other-hash", UUID.randomUUID())));

        assertThatThrownBy(() -> service.submit(request, "key-1"))
            .isInstanceOf(IdempotencyConflictException.class);
        verify(paymentRepository, never()).findById(any());
    }

    @Test
    void newKey_createsPayment() {
        when(paymentRepository.findByIdempotencyKey("key-new")).thenReturn(Optional.empty());
        PaymentResponse created = responseFor(UUID.randomUUID(), "PENDING");
        when(paymentWriteService.createPaymentAndEnqueue(eq(request), eq("key-new"), eq(fingerprint)))
            .thenReturn(created);

        PaymentResponse response = service.submit(request, "key-new");

        assertThat(response.id()).isEqualTo(created.id());
        verify(idempotencyService).cachePaymentIndex("key-new", fingerprint, created.id());
    }

    @Test
    void concurrentSameKey_sameFingerprint_returnsWinner() {
        Payment winner = paymentWithStatus(PaymentStatus.PENDING, fingerprint);
        when(paymentRepository.findByIdempotencyKey("key-race"))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(winner));
        when(paymentWriteService.createPaymentAndEnqueue(any(), eq("key-race"), eq(fingerprint)))
            .thenThrow(new DataIntegrityViolationException("unique"));

        PaymentResponse response = service.submit(request, "key-race");

        assertThat(response.id()).isEqualTo(winner.getId());
        assertThat(response.status()).isEqualTo("PENDING");
    }

    @Test
    void concurrentSameKey_differentFingerprint_throwsConflict() {
        String otherHash = RequestFingerprint.of(fromId, toId, new BigDecimal("1.00"), "USD");
        when(paymentRepository.findByIdempotencyKey("key-race"))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(paymentWithStatus(PaymentStatus.PENDING, otherHash)));
        when(paymentWriteService.createPaymentAndEnqueue(any(), eq("key-race"), eq(fingerprint)))
            .thenThrow(new DataIntegrityViolationException("unique"));

        assertThatThrownBy(() -> service.submit(request, "key-race"))
            .isInstanceOf(IdempotencyConflictException.class);
    }

    @Test
    void redisUnavailable_fallsBackToPostgres() {
        when(idempotencyService.getCachedEntry(any())).thenReturn(Optional.empty());
        Payment existing = paymentWithStatus(PaymentStatus.COMPLETED, fingerprint);
        when(paymentRepository.findByIdempotencyKey("key-r")).thenReturn(Optional.of(existing));

        PaymentResponse response = service.submit(request, "key-r");

        assertThat(response.status()).isEqualTo("COMPLETED");
        verify(paymentWriteService, never()).createPaymentAndEnqueue(any(), any(), any());
    }

    @Test
    void redisUnavailable_stillCreatesWhenNoExisting() {
        when(idempotencyService.getCachedEntry(any())).thenReturn(Optional.empty());
        when(paymentRepository.findByIdempotencyKey("key-r")).thenReturn(Optional.empty());
        PaymentResponse created = responseFor(UUID.randomUUID(), "PENDING");
        when(paymentWriteService.createPaymentAndEnqueue(eq(request), eq("key-r"), eq(fingerprint)))
            .thenReturn(created);

        PaymentResponse response = service.submit(request, "key-r");
        assertThat(response.id()).isEqualTo(created.id());
    }

    private Payment paymentWithStatus(PaymentStatus status, String hash) {
        return Payment.builder()
            .id(UUID.randomUUID())
            .fromAccountId(fromId)
            .toAccountId(toId)
            .amount(request.amount())
            .currency("USD")
            .status(status)
            .idempotencyKey("key-1")
            .requestHash(hash)
            .createdAt(Instant.now())
            .updatedAt(Instant.now())
            .build();
    }

    private PaymentResponse responseFor(UUID id, String status) {
        return new PaymentResponse(id, fromId, toId, request.amount(), "USD", status,
            null, Instant.now(), Instant.now());
    }

    @Test
    void blankKey_recordsRejectedMetric() {
        assertThatThrownBy(() -> service.submit(request, "   "))
            .isInstanceOf(InvalidIdempotencyKeyException.class);
        assertThat(meterRegistry.find("payment_ledger.payments.submitted")
            .tag("outcome", "rejected").counter().count()).isEqualTo(1.0);
    }

    @Test
    void conflict_recordsConflictMetric() {
        Payment existing = paymentWithStatus(PaymentStatus.PENDING, "other-hash");
        when(paymentRepository.findByIdempotencyKey("key-c")).thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> service.submit(request, "key-c"))
            .isInstanceOf(IdempotencyConflictException.class);
        assertThat(meterRegistry.find("payment_ledger.payments.submitted")
            .tag("outcome", "conflict").counter().count()).isEqualTo(1.0);
    }

    @Test
    void create_recordsCreatedMetric() {
        when(paymentRepository.findByIdempotencyKey("key-n")).thenReturn(Optional.empty());
        when(paymentWriteService.createPaymentAndEnqueue(eq(request), eq("key-n"), eq(fingerprint)))
            .thenReturn(responseFor(UUID.randomUUID(), "PENDING"));

        service.submit(request, "key-n");

        assertThat(meterRegistry.find("payment_ledger.payments.submitted")
            .tag("outcome", "created").counter().count()).isEqualTo(1.0);
    }

}
