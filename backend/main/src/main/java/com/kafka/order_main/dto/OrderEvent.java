package com.kafka.order_main.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderEvent(
        @NotNull(message = "Event ID cannot be null")
        UUID eventId,

        @NotBlank(message = "Order ID cannot be blank")
        String orderId,

        @NotBlank(message = "Product SKU cannot be blank")
        String productSku,

        @NotNull(message = "Amount cannot be null")
        @Positive(message = "Amount must be greater than zero")
        BigDecimal amount,

        @NotNull(message = "Timestamp cannot be null")
        Instant createdAt
) {
    public static OrderEvent create(String orderId, String productSku, BigDecimal amount) {
        return new OrderEvent(
                UUID.randomUUID(),
                orderId,
                productSku,
                amount,
                Instant.now()
        );
    }
}