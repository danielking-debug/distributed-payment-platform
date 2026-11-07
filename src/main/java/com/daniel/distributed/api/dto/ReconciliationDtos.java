package com.daniel.distributed.api.dto;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public final class ReconciliationDtos {

    private ReconciliationDtos() {}

    /**
     * Optional overrides for a single run. Null fields use application defaults.
     */
    public record RunRequest(
        /** Lookback window in hours (null = default). */
        Long lookbackHours,
        /** Stuck PENDING threshold in minutes (null = default). */
        Long stuckPendingMinutes,
        /** If set, only this payment is checked. */
        UUID paymentId
    ) {}

    public record Finding(
        UUID paymentId,
        String paymentStatus,
        String code,
        String severity,
        String detail
    ) {}

    public record RunResponse(
        Instant ranAt,
        long lookbackHours,
        long stuckPendingMinutes,
        int paymentsScanned,
        int findingCount,
        List<Finding> findings
    ) {}
}
