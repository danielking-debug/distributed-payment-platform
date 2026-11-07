package com.daniel.distributed.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.daniel.distributed.config.PaymentProperties;
import com.daniel.distributed.domain.*;
import com.daniel.distributed.kafka.event.EventTypes;
import com.daniel.distributed.kafka.event.PaymentCompletedEvent;
import com.daniel.distributed.kafka.event.PaymentFailedEvent;
import com.daniel.distributed.kafka.event.PaymentSubmittedEvent;
import com.daniel.distributed.observability.PaymentLedgerMetrics;
import com.daniel.distributed.observability.PaymentTracing;
import com.daniel.distributed.repository.*;
import io.micrometer.observation.ObservationRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Applies a submitted payment to the ledger. Invoked by the Kafka listener
 * for {@code payments.submitted}; every public method here runs inside one
 * database transaction that is either fully applied or fully rolled back —
 * there is no window where a balance is debited without a matching credit,
 * or where a ledger entry exists without an updated balance.
 *
 * <p>Concurrency safety: both accounts involved are locked with
 * {@code SELECT ... FOR UPDATE}, always in the same order — whichever
 * account id sorts first per {@link UUID#compareTo}. (Note that isn't the
 * same as naive lexicographic/numeric ordering: {@code UUID.compareTo}
 * compares the two 64-bit halves as <em>signed</em> longs, so a UUID whose
 * first hex digit is 8-f has its sign bit set and sorts as negative — e.g.
 * {@code ffff...} sorts before {@code 0000...}. All that matters here is
 * that the order is total and consistent, not which id "looks" bigger.)
 * Two transfers that both touch accounts A and B — even in opposite
 * directions — always acquire their locks in that same order, so neither
 * can deadlock waiting on the other.
 *
 * <p>Idempotency: before touching any balance, this checks
 * {@code processed_payment_events} for the payment id. Kafka's at-least-once
 * delivery means the same message can arrive more than once (a rebalance, a
 * crash between processing and offset commit); the insert into that table
 * happens in the same transaction as the ledger update, so a redelivered
 * message is a guaranteed no-op rather than a double-spend.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentProcessingService {

    private final AccountRepository accountRepository;
    private final PaymentRepository paymentRepository;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ProcessedPaymentEventRepository processedPaymentEventRepository;
    private final AccountService accountService;
    private final PaymentProperties paymentProperties;
    private final ObjectMapper objectMapper;
    private final ObservationRegistry observationRegistry;
    private final PaymentLedgerMetrics metrics;

    @Transactional
    public void process(PaymentSubmittedEvent event) {
        PaymentTracing.paymentProcess(observationRegistry, event.paymentId())
            .observe(() -> processObserved(event));
    }

    private void processObserved(PaymentSubmittedEvent event) {
        long start = System.nanoTime();
        boolean outcomeRecorded = false;
        try {
            if (processedPaymentEventRepository.existsById(event.paymentId())) {
                log.info("Payment {} already processed, skipping duplicate delivery", event.paymentId());
                metrics.paymentProcessedSkippedDuplicate(System.nanoTime() - start);
                outcomeRecorded = true;
                return;
            }

            Payment payment = paymentRepository.findById(event.paymentId())
                .orElseThrow(() -> new IllegalStateException("Outbox referenced unknown payment " + event.paymentId()));

            if (payment.getStatus() != PaymentStatus.PENDING) {
                log.info("Payment {} is not PENDING (status={}), skipping", payment.getId(), payment.getStatus());
                metrics.paymentProcessedSkippedNotPending(System.nanoTime() - start);
                outcomeRecorded = true;
                return;
            }

            // Claim work inside the same transaction as the ledger outcome.
            // PROCESSING is not durable on crash (tx rolls back to PENDING).
            payment.transitionTo(PaymentStatus.PROCESSING);

            UUID firstLockId = event.fromAccountId().compareTo(event.toAccountId()) <= 0
                ? event.fromAccountId() : event.toAccountId();
            UUID secondLockId = firstLockId.equals(event.fromAccountId()) ? event.toAccountId() : event.fromAccountId();

            Account first = accountRepository.findByIdForUpdate(firstLockId)
                .orElseThrow(() -> new IllegalStateException("Account " + firstLockId + " referenced by a payment no longer exists"));
            Account second = accountRepository.findByIdForUpdate(secondLockId)
                .orElseThrow(() -> new IllegalStateException("Account " + secondLockId + " referenced by a payment no longer exists"));

            Account from = first.getId().equals(event.fromAccountId()) ? first : second;
            Account to = first.getId().equals(event.toAccountId()) ? first : second;

            if (from.getBalance().compareTo(event.amount()) < 0) {
                failPayment(payment, "INSUFFICIENT_FUNDS");
                metrics.paymentProcessedFailed(System.nanoTime() - start);
                outcomeRecorded = true;
                return;
            }

            applyTransfer(payment, from, to);
            metrics.paymentProcessedCompleted(System.nanoTime() - start);
            outcomeRecorded = true;
        } catch (RuntimeException ex) {
            if (!outcomeRecorded) {
                // Timer only — not a business "failed" payment outcome.
                metrics.paymentProcessedError(System.nanoTime() - start);
            }
            throw ex;
        }
    }

    private void applyTransfer(Payment payment, Account from, Account to) {
        from.debit(payment.getAmount());
        to.credit(payment.getAmount());
        accountRepository.save(from);
        accountRepository.save(to);

        Instant now = Instant.now();
        ledgerEntryRepository.save(LedgerEntry.builder()
            .id(UUID.randomUUID())
            .accountId(from.getId())
            .paymentId(payment.getId())
            .entryType(EntryType.DEBIT)
            .amount(payment.getAmount())
            .balanceAfter(from.getBalance())
            .build());
        ledgerEntryRepository.save(LedgerEntry.builder()
            .id(UUID.randomUUID())
            .accountId(to.getId())
            .paymentId(payment.getId())
            .entryType(EntryType.CREDIT)
            .amount(payment.getAmount())
            .balanceAfter(to.getBalance())
            .build());

        payment.transitionTo(PaymentStatus.COMPLETED);
        paymentRepository.save(payment);

        processedPaymentEventRepository.save(new ProcessedPaymentEvent(payment.getId()));

        PaymentCompletedEvent completed = new PaymentCompletedEvent(
            payment.getId(), from.getId(), to.getId(), payment.getAmount(), payment.getCurrency(), now);
        enqueueOutbox(payment.getId(), EventTypes.PAYMENT_COMPLETED, paymentProperties.getCompletedTopic(),
            from.getId().toString(), completed);

        accountService.invalidateBalanceCache(from.getId());
        accountService.invalidateBalanceCache(to.getId());

        log.info("Payment {} completed: {} -> {} amount {}", payment.getId(), from.getId(), to.getId(), payment.getAmount());
    }

    private void failPayment(Payment payment, String reason) {
        payment.transitionTo(PaymentStatus.FAILED);
        payment.setFailureReason(reason);
        paymentRepository.save(payment);

        processedPaymentEventRepository.save(new ProcessedPaymentEvent(payment.getId()));

        PaymentFailedEvent failed = new PaymentFailedEvent(
            payment.getId(), payment.getFromAccountId(), payment.getToAccountId(),
            payment.getAmount(), payment.getCurrency(), reason, Instant.now());
        enqueueOutbox(payment.getId(), EventTypes.PAYMENT_FAILED, paymentProperties.getFailedTopic(),
            payment.getFromAccountId().toString(), failed);

        log.warn("Payment {} failed: {}", payment.getId(), reason);
    }

    private void enqueueOutbox(UUID paymentId, String eventType, String topic, String partitionKey, Object payload) {
        outboxEventRepository.save(OutboxEvent.builder()
            .id(UUID.randomUUID())
            .aggregateType("Payment")
            .aggregateId(paymentId)
            .eventType(eventType)
            .topic(topic)
            .partitionKey(partitionKey)
            .payload(writeJson(payload))
            .build());
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize outbox event payload", e);
        }
    }
}
