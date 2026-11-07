package com.daniel.distributed.service;

import com.daniel.distributed.api.dto.PaymentDtos.CreatePaymentRequest;
import com.daniel.distributed.api.dto.PaymentDtos.PaymentResponse;
import com.daniel.distributed.domain.Payment;
import com.daniel.distributed.exception.IdempotencyConflictException;
import com.daniel.distributed.exception.InvalidIdempotencyKeyException;
import com.daniel.distributed.observability.PaymentLedgerMetrics;
import com.daniel.distributed.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

/**
 * Handles the synchronous half of a payment: validate the Idempotency-Key,
 * bind it to a request fingerprint, then a single cheap local transaction
 * (delegated to {@link PaymentWriteService}) that records intent
 * (Payment=PENDING) and queues an outbox event for asynchronous ledger
 * processing.
 *
 * <p>Same key + same fingerprint → return the original payment (idempotent),
 * with {@linkplain #toResponse status always loaded from PostgreSQL}.
 * Same key + different fingerprint → {@link IdempotencyConflictException} (409).
 *
 * <p>Redis may accelerate key → payment id resolution; it is never the source
 * of truth for payment status.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentSubmissionService {

    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 255;

    private final PaymentRepository paymentRepository;
    private final PaymentWriteService paymentWriteService;
    private final IdempotencyService idempotencyService;
    private final PaymentLedgerMetrics metrics;

    public PaymentResponse submit(CreatePaymentRequest request, String idempotencyKey) {
        String key = validateIdempotencyKey(idempotencyKey);
        String fingerprint = RequestFingerprint.of(request);

        // Redis: fingerprint check + optional payment id. Status always from DB.
        var cached = idempotencyService.getCachedEntry(key);
        if (cached.isPresent()) {
            var entry = cached.get();
            if (!fingerprint.equals(entry.requestHash())) {
                metrics.paymentSubmittedConflict();
                throw new IdempotencyConflictException(key);
            }
            Payment payment = loadPaymentForReplay(key, entry.paymentId());
            if (payment != null) {
                log.debug("Idempotent replay for key {} (Redis index → Postgres status={})",
                    key, payment.getStatus());
                return respondFromDb(key, fingerprint, payment);
            }
            // Stale Redis index (payment missing): fall through to DB-by-key / create
        }

        var existing = paymentRepository.findByIdempotencyKey(key);
        if (existing.isPresent()) {
            return replayOrConflict(key, fingerprint, existing.get());
        }

        PaymentResponse response;
        try {
            response = paymentWriteService.createPaymentAndEnqueue(request, key, fingerprint);
        } catch (DataIntegrityViolationException e) {
            Payment winner = paymentRepository.findByIdempotencyKey(key)
                .orElseThrow(() -> e);
            return replayOrConflict(key, fingerprint, winner);
        }

        idempotencyService.cachePaymentIndex(key, fingerprint, response.id());
        metrics.paymentSubmittedCreated();
        return response;
    }

    /**
     * Prefer PK lookup when Redis supplied a payment id; otherwise unique key lookup.
     */
    private Payment loadPaymentForReplay(String key, java.util.UUID paymentId) {
        if (paymentId != null) {
            var byId = paymentRepository.findById(paymentId);
            if (byId.isPresent()) {
                return byId.get();
            }
        }
        return paymentRepository.findByIdempotencyKey(key).orElse(null);
    }

    private PaymentResponse replayOrConflict(String key, String fingerprint, Payment existing) {
        if (existing.getRequestHash() != null && !fingerprint.equals(existing.getRequestHash())) {
            metrics.paymentSubmittedConflict();
            throw new IdempotencyConflictException(key);
        }
        return respondFromDb(key, fingerprint, existing);
    }

    private PaymentResponse respondFromDb(String key, String fingerprint, Payment payment) {
        PaymentResponse response = toResponse(payment);
        idempotencyService.cachePaymentIndex(key, fingerprint, payment.getId());
        metrics.paymentSubmittedReplayed();
        return response;
    }

    /**
     * Validation boundary for Idempotency-Key. Rejection metrics are recorded here
     * so each invalid key is counted exactly once before the exception propagates.
     */
    String validateIdempotencyKey(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            metrics.paymentSubmittedRejected();
            throw new InvalidIdempotencyKeyException(
                "Idempotency-Key header is required and must not be blank");
        }
        String trimmed = idempotencyKey.trim();
        if (trimmed.length() > MAX_IDEMPOTENCY_KEY_LENGTH) {
            metrics.paymentSubmittedRejected();
            throw new InvalidIdempotencyKeyException(
                "Idempotency-Key must be at most " + MAX_IDEMPOTENCY_KEY_LENGTH + " characters");
        }
        return trimmed;
    }

    private PaymentResponse toResponse(Payment payment) {
        return new PaymentResponse(payment.getId(), payment.getFromAccountId(), payment.getToAccountId(),
            payment.getAmount(), payment.getCurrency(), payment.getStatus().name(), payment.getFailureReason(),
            payment.getCreatedAt(), payment.getUpdatedAt());
    }
}
