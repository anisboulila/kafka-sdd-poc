package com.example.kafkasddpoc.order;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrderService orderService;

    @Test
    void returnsCreatedOrderAfterServicePublicationCompletes() throws Exception {
        when(orderService.createOrder(any(CreateOrderRequest.class))).thenReturn(
                new OrderCreated(
                        "order-123",
                        "customer-456",
                        new BigDecimal("19.95"),
                        "2026-10-05T16:00:00Z"));

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerId":"customer-456","amount":19.95}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").value("order-123"))
                .andExpect(jsonPath("$.customerId").value("customer-456"))
                .andExpect(jsonPath("$.amount").value(19.95))
                .andExpect(jsonPath("$.createdAt").value("2026-10-05T16:00:00Z"));
    }
}
