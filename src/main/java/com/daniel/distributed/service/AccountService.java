package com.daniel.distributed.service;

import com.daniel.distributed.api.dto.AccountDtos.AccountResponse;
import com.daniel.distributed.api.dto.AccountDtos.CreateAccountRequest;
import com.daniel.distributed.api.dto.AccountDtos.LedgerEntryResponse;
import com.daniel.distributed.config.PaymentProperties;
import com.daniel.distributed.domain.Account;
import com.daniel.distributed.domain.LedgerEntry;
import com.daniel.distributed.exception.AccountNotFoundException;
import com.daniel.distributed.repository.AccountRepository;
import com.daniel.distributed.repository.LedgerEntryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class AccountService {

    /**
     * Legacy key prefix. Mutable balances are no longer served from Redis;
     * keys may still be deleted after ledger commits to clear any leftover
     * entries from earlier versions of the service.
     */
    static final String BALANCE_CACHE_PREFIX = "balance:account:";

    private final AccountRepository accountRepository;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final StringRedisTemplate redisTemplate;
    private final PaymentProperties paymentProperties;

    @Transactional
    public AccountResponse createAccount(CreateAccountRequest request) {
        Account account = Account.builder()
            .id(UUID.randomUUID())
            .ownerName(request.ownerName())
            .currency(request.currency())
            .balance(request.initialBalance())
            .build();
        accountRepository.save(account);
        return toResponse(account);
    }

    /**
     * Account balances are always read from PostgreSQL (source of truth).
     *
     * <p>Earlier cache-aside Redis balance caching was removed: {@code getAccount}
     * always loaded the account row from the database, then optionally overrode
     * the balance with a Redis value and, on a miss, wrote the DB balance back
     * into Redis. That write-after-read raced with post-commit invalidation:
     * a concurrent transfer could commit and delete the key between the DB read
     * and the Redis SET, so the stale pre-transfer balance was repopulated and
     * later GETs returned it until TTL. Correctness takes priority over caching
     * a field that changes under row locks on every transfer.
     */
    public AccountResponse getAccount(UUID accountId) {
        Account account = accountRepository.findById(accountId)
            .orElseThrow(() -> new AccountNotFoundException(accountId));
        return toResponse(account);
    }

    /**
     * Best-effort cleanup of any leftover balance cache keys. Deferred until
     * after a successful commit when called inside a transaction so it cannot
     * interact with an in-flight ledger update. No longer required for
     * correctness of {@link #getAccount}, which does not read Redis.
     */
    public void invalidateBalanceCache(UUID accountId) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    deleteBalanceCache(accountId);
                }
            });
        } else {
            deleteBalanceCache(accountId);
        }
    }

    void deleteBalanceCache(UUID accountId) {
        redisTemplate.delete(BALANCE_CACHE_PREFIX + accountId);
    }

    /**
     * Plain DB reads — backs the frontend account picker/list.
     */
    public List<AccountResponse> listAccounts() {
        return accountRepository.findAll(Sort.by(Sort.Direction.ASC, "createdAt")).stream()
            .map(this::toResponse)
            .toList();
    }

    public Page<LedgerEntryResponse> getLedger(UUID accountId, Pageable pageable) {
        if (!accountRepository.existsById(accountId)) {
            throw new AccountNotFoundException(accountId);
        }
        return ledgerEntryRepository.findByAccountIdOrderByCreatedAtDesc(accountId, pageable)
            .map(this::toLedgerResponse);
    }

    private AccountResponse toResponse(Account account) {
        return new AccountResponse(account.getId(), account.getOwnerName(), account.getCurrency(),
            account.getBalance(), account.getCreatedAt());
    }

    private LedgerEntryResponse toLedgerResponse(LedgerEntry entry) {
        return new LedgerEntryResponse(entry.getId(), entry.getAccountId(), entry.getPaymentId(),
            entry.getEntryType().name(), entry.getAmount(), entry.getBalanceAfter(), entry.getCreatedAt());
    }
}
