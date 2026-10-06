package com.example.kafkasddpoc.order;

import java.math.BigDecimal;

public record CreateOrderRequest(String customerId, BigDecimal amount) {
}
