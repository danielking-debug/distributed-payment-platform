package com.daniel.distributed.domain;

/**
 * Payment lifecycle states.
 *
 * <pre>
 *   PENDING → PROCESSING → COMPLETED
 *                        → FAILED
 * </pre>
 *
 * Transitions are enforced by {@link Payment#transitionTo(PaymentStatus)}.
 * New payments are created directly as {@code PENDING}.
 */
public enum PaymentStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    FAILED
}
