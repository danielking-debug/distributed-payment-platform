package com.daniel.distributed.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.daniel.distributed.config.PaymentProperties;
import com.daniel.distributed.domain.*;
import com.daniel.distributed.kafka.event.PaymentSubmittedEvent;
import com.daniel.distributed.repository.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import com.daniel.distributed.observability.PaymentLedgerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.mockito.MockitoAnnotations;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PaymentProcessingServiceTest {

    @Mock private AccountRepository accountRepository;
    @Mock private PaymentRepository paymentRepository;
    @Mock private LedgerEntryRepository ledgerEntryRepository;
    @Mock private OutboxEventRepository outboxEventRepository;
    @Mock private ProcessedPaymentEventRepository processedPaymentEventRepository;
    @Mock private AccountService accountService;

    private PaymentProcessingService service;
    private SimpleMeterRegistry meterRegistry;
    private PaymentLedgerMetrics metrics;

    private Account from;
    private Account to;
    private Payment payment;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        meterRegistry = new SimpleMeterRegistry();
        metrics = new PaymentLedgerMetrics(meterRegistry);
        service = new PaymentProcessingService(
            accountRepository, paymentRepository, ledgerEntryRepository, outboxEventRepository,
            processedPaymentEventRepository, accountService, new PaymentProperties(), objectMapper,
            ObservationRegistry.NOOP, metrics);

        UUID fromId = UUID.randomUUID();
        UUID toId = UUID.randomUUID();
        from = Account.builder().id(fromId).ownerName("Alice").currency("USD").balance(new BigDecimal("100.0000")).build();
        to = Account.builder().id(toId).ownerName("Bob").currency("USD").balance(new BigDecimal("50.0000")).build();

        payment = Payment.builder()
            .id(UUID.randomUUID())
            .fromAccountId(fromId)
            .toAccountId(toId)
            .amount(new BigDecimal("30.0000"))
            .currency("USD")
            .status(PaymentStatus.PENDING)
            .build();

        when(processedPaymentEventRepository.existsById(payment.getId())).thenReturn(false);
        when(paymentRepository.findById(payment.getId())).thenReturn(Optional.of(payment));
        lenientAccountLookups();
    }

    private void lenientAccountLookups() {
        when(accountRepository.findByIdForUpdate(from.getId())).thenReturn(Optional.of(from));
        when(accountRepository.findByIdForUpdate(to.getId())).thenReturn(Optional.of(to));
    }

    private PaymentSubmittedEvent submittedEvent() {
        return new PaymentSubmittedEvent(payment.getId(), from.getId(), to.getId(), payment.getAmount(),
            payment.getCurrency(), Instant.now());
    }

    @Test
    void sufficientFunds_debitsCreditsAndCompletesPayment() {
        service.process(submittedEvent());

        assertThat(from.getBalance()).isEqualByComparingTo("70.0000");
        assertThat(to.getBalance()).isEqualByComparingTo("80.0000");
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.COMPLETED);

        ArgumentCaptor<LedgerEntry> ledgerCaptor = ArgumentCaptor.forClass(LedgerEntry.class);
        verify(ledgerEntryRepository, times(2)).save(ledgerCaptor.capture());
        assertThat(ledgerCaptor.getAllValues())
            .extracting(LedgerEntry::getEntryType)
            .containsExactlyInAnyOrder(EntryType.DEBIT, EntryType.CREDIT);

        verify(processedPaymentEventRepository).save(argThat(p -> p.getPaymentId().equals(payment.getId())));
        verify(outboxEventRepository).save(argThat(e -> e.getEventType().equals("PAYMENT_COMPLETED")));
        verify(accountService).invalidateBalanceCache(from.getId());
        verify(accountService).invalidateBalanceCache(to.getId());
    }

    @Test
    void insufficientFunds_failsPaymentWithoutTouchingBalances() {
        payment.setAmount(new BigDecimal("500.0000"));

        service.process(new PaymentSubmittedEvent(payment.getId(), from.getId(), to.getId(),
            payment.getAmount(), payment.getCurrency(), Instant.now()));

        assertThat(from.getBalance()).isEqualByComparingTo("100.0000"); // untouched
        assertThat(to.getBalance()).isEqualByComparingTo("50.0000");    // untouched
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(payment.getFailureReason()).isEqualTo("INSUFFICIENT_FUNDS");

        verify(ledgerEntryRepository, never()).save(any());
        verify(outboxEventRepository).save(argThat(e -> e.getEventType().equals("PAYMENT_FAILED")));
    }

    @Test
    void alreadyProcessedEvent_isSkippedForIdempotency() {
        when(processedPaymentEventRepository.existsById(payment.getId())).thenReturn(true);

        service.process(submittedEvent());

        verifyNoInteractions(ledgerEntryRepository);
        verify(accountRepository, never()).findByIdForUpdate(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void nonPendingPayment_isSkipped() {
        payment.setStatus(PaymentStatus.COMPLETED);

        service.process(submittedEvent());

        verifyNoInteractions(ledgerEntryRepository);
        verify(accountRepository, never()).findByIdForUpdate(any());
    }

    @Test
    void accountLocksAreAlwaysAcquiredInAConsistentOrder_evenWhenPaymentDirectionIsReversed() {
        // Force `to` to have the id that sorts first (per UUID.compareTo,
        // which compares the two 64-bit halves as SIGNED longs — a UUID
        // starting with 8-f has its sign bit set and sorts as negative, so
        // "0000...1" sorts before "7fff...", not "ffff..." as naive
        // lexicographic intuition would suggest). `to` must be locked FIRST
        // here even though it is the *destination*, not the source, of this
        // payment — that's what prevents two transfers moving money in
        // opposite directions between the same two accounts from deadlocking.
        UUID lowId = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID highId = UUID.fromString("7fffffff-ffff-ffff-ffff-ffffffffffff");
        to.setId(lowId);
        from.setId(highId);
        payment.setFromAccountId(highId);
        payment.setToAccountId(lowId);
        lenientAccountLookups();

        service.process(submittedEvent());

        var inOrder = inOrder(accountRepository);
        inOrder.verify(accountRepository).findByIdForUpdate(lowId);
        inOrder.verify(accountRepository).findByIdForUpdate(highId);
    }

    @Test
    void unexpectedException_recordsErrorTimerOnly_andPropagates() {
        when(processedPaymentEventRepository.existsById(payment.getId())).thenReturn(false);
        when(paymentRepository.findById(payment.getId())).thenReturn(Optional.of(payment));
        when(accountRepository.findByIdForUpdate(from.getId()))
            .thenThrow(new RuntimeException("db exploded"));

        PaymentSubmittedEvent event = new PaymentSubmittedEvent(
            payment.getId(), from.getId(), to.getId(), payment.getAmount(), payment.getCurrency(), Instant.now());

        assertThatThrownBy(() -> service.process(event))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("db exploded");

        assertThat(meterRegistry.find("payment_ledger.payments.processing")
            .tag("outcome", "error").timer()).isNotNull();
        assertThat(meterRegistry.find("payment_ledger.payments.processing")
            .tag("outcome", "error").timer().count()).isEqualTo(1);
        // business failed counter must not increment
        assertThat(meterRegistry.find("payment_ledger.payments.processed")
            .tag("outcome", "failed").counter()).isNull();
    }
}
