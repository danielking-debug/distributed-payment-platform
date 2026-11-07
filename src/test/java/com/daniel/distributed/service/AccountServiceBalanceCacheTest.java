package com.daniel.distributed.service;

import com.daniel.distributed.config.PaymentProperties;
import com.daniel.distributed.domain.Account;
import com.daniel.distributed.repository.AccountRepository;
import com.daniel.distributed.repository.LedgerEntryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Regression coverage for balance-cache races.
 *
 * <p>Previously {@code getAccount} preferred a Redis balance when present and,
 * on a miss, wrote the Postgres balance back into Redis. Ordering:
 * <ol>
 *   <li>GET reads Postgres balance B_old</li>
 *   <li>Transfer commits; afterCommit deletes the Redis key</li>
 *   <li>GET observes a miss and SETs B_old into Redis</li>
 *   <li>Later GETs return B_old until TTL</li>
 * </ol>
 * Mutable balances are no longer cached; these tests lock that in.
 */
class AccountServiceBalanceCacheTest {

    @Mock AccountRepository accountRepository;
    @Mock LedgerEntryRepository ledgerEntryRepository;
    @Mock StringRedisTemplate redisTemplate;
    @Mock ValueOperations<String, String> valueOps;
    @Mock PaymentProperties paymentProperties;

    AccountService service;
    UUID accountId;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        service = new AccountService(accountRepository, ledgerEntryRepository, redisTemplate, paymentProperties);
        accountId = UUID.randomUUID();
    }

    @Test
    void getAccount_alwaysReturnsPostgresBalance_andNeverReadsOrWritesRedis() {
        Account account = Account.builder()
            .id(accountId)
            .ownerName("A")
            .currency("USD")
            .balance(new BigDecimal("9750.00"))
            .createdAt(Instant.now())
            .build();
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(account));
        // If Redis were consulted, a stale value would win — it must not be used.
        when(valueOps.get(AccountService.BALANCE_CACHE_PREFIX + accountId)).thenReturn("9760.00");

        var response = service.getAccount(accountId);

        assertThat(response.balance()).isEqualByComparingTo("9750.00");
        verify(redisTemplate, never()).opsForValue();
        verify(valueOps, never()).get(anyString());
        verify(valueOps, never()).set(anyString(), anyString());
        verify(valueOps, never()).set(anyString(), anyString(), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void getAccount_doesNotRepopulateRedisAfterConcurrentInvalidation() throws Exception {
        // Simulates the historical race ordering without a real Redis/Postgres.
        // Thread GET: loads B_old from DB, then (previously) would SET Redis.
        // Thread TX: afterCommit deletes the key between those two steps.
        Account oldView = Account.builder()
            .id(accountId)
            .ownerName("A")
            .currency("USD")
            .balance(new BigDecimal("9760.00"))
            .createdAt(Instant.now())
            .build();

        CountDownLatch dbReadDone = new CountDownLatch(1);
        CountDownLatch invalidationDone = new CountDownLatch(1);
        AtomicReference<BigDecimal> returned = new AtomicReference<>();

        when(accountRepository.findById(accountId)).thenAnswer(inv -> {
            dbReadDone.countDown();
            assertThat(invalidationDone.await(2, TimeUnit.SECONDS)).isTrue();
            return Optional.of(oldView);
        });

        Thread reader = new Thread(() -> returned.set(service.getAccount(accountId).balance()));
        reader.start();
        assertThat(dbReadDone.await(2, TimeUnit.SECONDS)).isTrue();

        // Concurrent transfer finished: after-commit invalidation runs.
        service.deleteBalanceCache(accountId);
        invalidationDone.countDown();
        reader.join(2000);

        // Response may still reflect the DB snapshot this call started with,
        // but Redis must never be written with that snapshot after invalidation.
        assertThat(returned.get()).isEqualByComparingTo("9760.00");
        verify(valueOps, never()).set(anyString(), anyString());
        verify(valueOps, never()).set(anyString(), anyString(), org.mockito.ArgumentMatchers.any());
        verify(redisTemplate, times(1)).delete(AccountService.BALANCE_CACHE_PREFIX + accountId);
    }

    @Test
    void invalidateOutsideTransaction_deletesImmediately() {
        assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isFalse();

        service.invalidateBalanceCache(accountId);

        verify(redisTemplate, times(1)).delete(AccountService.BALANCE_CACHE_PREFIX + accountId);
    }

    @Test
    void invalidateInsideTransaction_defersDeleteUntilAfterCommit() {
        TransactionSynchronizationManager.initSynchronization();
        try {
            service.invalidateBalanceCache(accountId);
            verify(redisTemplate, never()).delete(anyString());

            for (TransactionSynchronization sync : TransactionSynchronizationManager.getSynchronizations()) {
                sync.afterCommit();
            }

            verify(redisTemplate, times(1)).delete(AccountService.BALANCE_CACHE_PREFIX + accountId);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void invalidateInsideTransaction_rollbackDoesNotDelete() {
        TransactionSynchronizationManager.initSynchronization();
        try {
            service.invalidateBalanceCache(accountId);
            verify(redisTemplate, never()).delete(anyString());

            for (TransactionSynchronization sync : TransactionSynchronizationManager.getSynchronizations()) {
                sync.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);
            }

            verify(redisTemplate, never()).delete(anyString());
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }
}
