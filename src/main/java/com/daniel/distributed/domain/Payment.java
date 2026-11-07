package com.daniel.distributed.domain;

import com.daniel.distributed.exception.IllegalPaymentStateTransitionException;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payments")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Payment {

    @Id
    private UUID id;

    @Column(name = "from_account_id", nullable = false)
    private UUID fromAccountId;

    @Column(name = "to_account_id", nullable = false)
    private UUID toAccountId;

    @Column(name = "amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private PaymentStatus status;

    @Column(name = "idempotency_key")
    private String idempotencyKey;

    /** SHA-256 hex binding Idempotency-Key to from/to/amount/currency. */
    @Column(name = "request_hash", length = 64)
    private String requestHash;

    @Column(name = "failure_reason")
    private String failureReason;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    /**
     * Enforces the payment lifecycle state machine:
     * <pre>
     *   PENDING → PROCESSING → COMPLETED
     *                        → FAILED
     * </pre>
     * All other transitions throw {@link com.daniel.distributed.exception.IllegalPaymentStateTransitionException}.
     * New payments are born as {@code PENDING} via the builder; that initial
     * assignment does not go through this method.
     */
    public void transitionTo(PaymentStatus target) {
        if (!isAllowedTransition(this.status, target)) {
            throw new IllegalPaymentStateTransitionException(this.id, this.status, target);
        }
        this.status = target;
    }

    private static boolean isAllowedTransition(PaymentStatus from, PaymentStatus to) {
        if (from == null || to == null) {
            return false;
        }
        return switch (from) {
            case PENDING -> to == PaymentStatus.PROCESSING;
            case PROCESSING -> to == PaymentStatus.COMPLETED || to == PaymentStatus.FAILED;
            case COMPLETED, FAILED -> false;
        };
    }
}
