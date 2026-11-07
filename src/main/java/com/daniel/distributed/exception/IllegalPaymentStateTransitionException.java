package com.daniel.distributed.exception;

import com.daniel.distributed.domain.PaymentStatus;

import java.util.UUID;

/**
 * Thrown when code attempts a payment status change that is not allowed by
 * the state machine. Fail-fast: illegal transitions indicate a programming
 * error, not a recoverable business condition.
 */
public class IllegalPaymentStateTransitionException extends RuntimeException {

    public IllegalPaymentStateTransitionException(UUID paymentId, PaymentStatus from, PaymentStatus to) {
        super("Illegal payment transition for " + paymentId + ": " + from + " → " + to);
    }
}
