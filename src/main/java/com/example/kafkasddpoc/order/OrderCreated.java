package com.example.kafkasddpoc.order;

import java.math.BigDecimal;

public record OrderCreated(
        String orderId,
        String customerId,
        BigDecimal amount,
        String createdAt) {
}
