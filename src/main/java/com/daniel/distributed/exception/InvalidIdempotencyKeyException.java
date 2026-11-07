package com.daniel.distributed.exception;

/**
 * Thrown when the Idempotency-Key header is missing, blank, or exceeds the maximum length.
 */
public class InvalidIdempotencyKeyException extends RuntimeException {
    public InvalidIdempotencyKeyException(String message) {
        super(message);
    }
}
