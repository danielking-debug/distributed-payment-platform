package com.daniel.distributed.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.daniel.distributed.api.dto.PaymentDtos.CreatePaymentRequest;
import com.daniel.distributed.api.dto.PaymentDtos.PaymentResponse;
import com.daniel.distributed.config.PaymentProperties;
import com.daniel.distributed.domain.Account;
import com.daniel.distributed.domain.OutboxEvent;
import com.daniel.distributed.domain.Payment;
import com.daniel.distributed.domain.PaymentStatus;
import com.daniel.distributed.exception.AccountNotFoundException;
import com.daniel.distributed.exception.CurrencyMismatchException;
import com.daniel.distributed.kafka.event.EventTypes;
import com.daniel.distributed.kafka.event.PaymentSubmittedEvent;
import com.daniel.distributed.repository.AccountRepository;
import com.daniel.distributed.repository.OutboxEventRepository;
import com.daniel.distributed.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Kept as its own Spring bean (rather than a method on
 * {@link PaymentSubmissionService}) purely so that {@code @Transactional}
 * is honored: Spring's proxy-based transactions only intercept calls that
 * arrive through the bean proxy, and a method calling another method on
 * {@code this} bypasses that proxy entirely (the classic "self-invocation"
 * pitfall). Splitting the transactional write into a separate, injected
 * collaborator sidesteps that.
 */
@Service
@RequiredArgsConstructor
public class PaymentWriteService {

    private final AccountRepository accountRepository;
    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final PaymentProperties paymentProperties;
    private final ObjectMapper objectMapper;

    @Transactional
    public PaymentResponse createPaymentAndEnqueue(
            CreatePaymentRequest request, String idempotencyKey, String requestHash) {
        Account from = accountRepository.findById(request.fromAccountId())
            .orElseThrow(() -> new AccountNotFoundException(request.fromAccountId()));
        Account to = accountRepository.findById(request.toAccountId())
            .orElseThrow(() -> new AccountNotFoundException(request.toAccountId()));

        if (!from.getCurrency().equals(request.currency()) || !to.getCurrency().equals(request.currency())) {
            throw new CurrencyMismatchException(
                "Payment currency " + request.currency() + " does not match account currency");
        }

        Payment payment = Payment.builder()
            .id(UUID.randomUUID())
            .fromAccountId(from.getId())
            .toAccountId(to.getId())
            .amount(request.amount())
            .currency(request.currency())
            .status(PaymentStatus.PENDING)
            .idempotencyKey(idempotencyKey)
            .requestHash(requestHash)
            .build();
        paymentRepository.save(payment);

        PaymentSubmittedEvent event = new PaymentSubmittedEvent(
            payment.getId(), from.getId(), to.getId(), payment.getAmount(), payment.getCurrency(), Instant.now());

        OutboxEvent outboxEvent = OutboxEvent.builder()
            .id(UUID.randomUUID())
            .aggregateType("Payment")
            .aggregateId(payment.getId())
            .eventType(EventTypes.PAYMENT_SUBMITTED)
            .topic(paymentProperties.getSubmittedTopic())
            // Keying by the source account keeps all of one account's outgoing
            // payments in the same Kafka partition, so they are processed in
            // the order they were submitted.
            .partitionKey(from.getId().toString())
            .payload(writeJson(event))
            .build();
        outboxEventRepository.save(outboxEvent);

        return toResponse(payment);
    }

    private PaymentResponse toResponse(Payment payment) {
        return new PaymentResponse(payment.getId(), payment.getFromAccountId(), payment.getToAccountId(),
            payment.getAmount(), payment.getCurrency(), payment.getStatus().name(), payment.getFailureReason(),
            payment.getCreatedAt(), payment.getUpdatedAt());
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize outbox event payload", e);
        }
    }
}
