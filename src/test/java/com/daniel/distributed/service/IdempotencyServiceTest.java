package com.daniel.distributed.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.daniel.distributed.config.PaymentProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class IdempotencyServiceTest {

    @Mock private StringRedisTemplate redisTemplate;
    @Mock private ValueOperations<String, String> valueOperations;

    private IdempotencyService service;
    private final Map<String, String> fakeStore = new HashMap<>();

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(anyString())).thenAnswer(inv -> fakeStore.get(inv.getArgument(0)));
        org.mockito.Mockito.doAnswer(inv -> {
            fakeStore.put(inv.getArgument(0), inv.getArgument(1));
            return null;
        }).when(valueOperations).set(anyString(), anyString(), any(Duration.class));

        service = new IdempotencyService(redisTemplate, objectMapper, new PaymentProperties());
    }

    @Test
    void cacheMiss_returnsEmpty() {
        assertThat(service.getCachedEntry("unknown-key")).isEmpty();
    }

    @Test
    void cachedEntry_storesHashAndPaymentId_notStatus() {
        UUID paymentId = UUID.randomUUID();
        String hash = "abc123hash";

        service.cachePaymentIndex("key-123", hash, paymentId);
        Optional<IdempotencyService.CachedEntry> replayed = service.getCachedEntry("key-123");

        assertThat(replayed).isPresent();
        assertThat(replayed.get().requestHash()).isEqualTo(hash);
        assertThat(replayed.get().paymentId()).isEqualTo(paymentId);
    }

    @Test
    void redisFailureOnRead_returnsEmpty() {
        when(valueOperations.get(anyString())).thenThrow(new RuntimeException("Redis down"));
        assertThat(service.getCachedEntry("key")).isEmpty();
    }

    @Test
    void redisFailureOnWrite_doesNotThrow() {
        org.mockito.Mockito.doThrow(new RuntimeException("Redis down"))
            .when(valueOperations).set(anyString(), anyString(), any(Duration.class));
        service.cachePaymentIndex("key", "hash", UUID.randomUUID());
    }
}
