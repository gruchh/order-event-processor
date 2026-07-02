package com.kafka.order_main.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;

public record CreateOrderRequest(
        @NotBlank(message = "Order ID cannot be blank")
        String orderId,

        @NotBlank(message = "Product SKU cannot be blank")
        String productSku,

        @NotNull(message = "Amount cannot be null")
        BigDecimal amount
) {}