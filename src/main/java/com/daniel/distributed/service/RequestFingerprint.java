package com.daniel.distributed.service;

import com.daniel.distributed.api.dto.PaymentDtos.CreatePaymentRequest;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

/**
 * Stable hash of the payment intent fields used to bind an Idempotency-Key
 * to a specific request payload. The key itself is intentionally excluded.
 */
public final class RequestFingerprint {

    private RequestFingerprint() {}

    public static String of(CreatePaymentRequest request) {
        return of(request.fromAccountId(), request.toAccountId(), request.amount(), request.currency());
    }

    public static String of(UUID fromAccountId, UUID toAccountId, BigDecimal amount, String currency) {
        // toPlainString avoids scientific notation; uppercase currency for stability
        String material = fromAccountId
            + "|" + toAccountId
            + "|" + amount.toPlainString()
            + "|" + currency.trim().toUpperCase();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(material.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
