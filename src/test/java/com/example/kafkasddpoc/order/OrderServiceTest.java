package com.example.kafkasddpoc.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    private static final String ORDER_EVENTS_TOPIC = "order-events";
    private static final String CREATED_AT = "2026-10-05T16:00:00Z";

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void waitsForKafkaConfirmationBeforeReturningCreatedEvent() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Clock clock = Clock.fixed(Instant.parse(CREATED_AT), ZoneOffset.UTC);
        OrderService orderService = new OrderService(kafkaTemplate, objectMapper, clock);
        CompletableFuture<SendResult<String, String>> publication = new CompletableFuture<>();
        when(kafkaTemplate.send(eq(ORDER_EVENTS_TOPIC), anyString(), anyString()))
                .thenReturn(publication);

        CompletableFuture<OrderCreated> order = CompletableFuture.supplyAsync(
                () -> orderService.createOrder(
                        new CreateOrderRequest("customer-456", new BigDecimal("19.95"))));

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, timeout(1000))
                .send(eq(ORDER_EVENTS_TOPIC), key.capture(), payload.capture());

        assertFalse(order.isDone());
        assertTrue(key.getValue().matches("[0-9a-fA-F-]{36}"));

        JsonNode json = objectMapper.readTree(payload.getValue());
        assertEquals(key.getValue(), json.get("orderId").asText());
        assertEquals("customer-456", json.get("customerId").asText());
        assertEquals(new BigDecimal("19.95"), json.get("amount").decimalValue());
        assertEquals(CREATED_AT, json.get("createdAt").asText());

        publication.complete(null);
        OrderCreated createdOrder = order.get(1, TimeUnit.SECONDS);

        assertEquals(key.getValue(), createdOrder.orderId());
        assertEquals("customer-456", createdOrder.customerId());
        assertEquals(new BigDecimal("19.95"), createdOrder.amount());
        assertEquals(CREATED_AT, createdOrder.createdAt());
    }
}
