package com.daniel.distributed.exception;

/**
 * Thrown when a manual DLT replay request fails validation
 * (unknown topic, malformed payload, missing key, etc.).
 */
public class InvalidDltReplayException extends RuntimeException {
    public InvalidDltReplayException(String message) {
        super(message);
    }

    public InvalidDltReplayException(String message, Throwable cause) {
        super(message, cause);
    }
}
