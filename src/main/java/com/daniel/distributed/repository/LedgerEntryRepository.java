package com.daniel.distributed.repository;

import com.daniel.distributed.domain.LedgerEntry;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, UUID> {

    Page<LedgerEntry> findByAccountIdOrderByCreatedAtDesc(UUID accountId, Pageable pageable);

    List<LedgerEntry> findByPaymentId(UUID paymentId);

    /** Bulk load ledger rows for many payments (reconciliation; avoids N+1). */
    List<LedgerEntry> findByPaymentIdIn(Collection<UUID> paymentIds);
}
