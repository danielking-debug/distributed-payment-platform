package com.daniel.distributed.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Defaults for payment/ledger reconciliation scans.
 * Lookback limits how far back terminal payments are checked;
 * stuck-pending threshold flags aged PENDING without treating young async work as errors.
 */
@ConfigurationProperties(prefix = "app.reconciliation")
public class ReconciliationProperties {

    /** How far back to scan payments (hours). Default 24. */
    private long lookbackHours = 24;

    /** PENDING older than this many minutes is reported as STUCK_PENDING. Default 30. */
    private long stuckPendingMinutes = 30;

    public long getLookbackHours() {
        return lookbackHours;
    }

    public void setLookbackHours(long lookbackHours) {
        this.lookbackHours = lookbackHours;
    }

    public long getStuckPendingMinutes() {
        return stuckPendingMinutes;
    }

    public void setStuckPendingMinutes(long stuckPendingMinutes) {
        this.stuckPendingMinutes = stuckPendingMinutes;
    }
}
