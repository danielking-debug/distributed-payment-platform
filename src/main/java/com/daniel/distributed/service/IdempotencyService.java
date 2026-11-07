package com.daniel.distributed.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.daniel.distributed.config.PaymentProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Best-effort Redis index for idempotent payment submission.
 * Caches {@code Idempotency-Key → (requestHash, paymentId)} only — never a
 * frozen status snapshot. Payment status is always read from PostgreSQL on
 * replay. Redis failures never fail the request.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class IdempotencyService {

    private static final String KEY_PREFIX = "idempotency:payment:";

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final PaymentProperties paymentProperties;

    /**
     * @return cached index entry if present and readable; empty on miss or Redis errors
     */
    public Optional<CachedEntry> getCachedEntry(String idempotencyKey) {
        try {
            String json = redisTemplate.opsForValue().get(KEY_PREFIX + idempotencyKey);
            if (json == null) {
                return Optional.empty();
            }
            return Optional.of(objectMapper.readValue(json, CachedEntry.class));
        } catch (Exception e) {
            log.warn("Failed to read idempotency cache for key {}: {}", idempotencyKey, e.toString());
            return Optional.empty();
        }
    }

    public void cachePaymentIndex(String idempotencyKey, String requestHash, UUID paymentId) {
        try {
            CachedEntry entry = new CachedEntry(requestHash, paymentId);
            String json = objectMapper.writeValueAsString(entry);
            redisTemplate.opsForValue().set(
                KEY_PREFIX + idempotencyKey,
                json,
                Duration.ofHours(paymentProperties.getIdempotencyTtlHours()));
        } catch (Exception e) {
            log.warn("Failed to cache idempotency index for key {}: {}", idempotencyKey, e.toString());
        }
    }

    /**
     * Redis index only: fingerprint binding + payment id for lookup.
     * Does not store status or a full {@code PaymentResponse}.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CachedEntry(String requestHash, UUID paymentId) {}
}
