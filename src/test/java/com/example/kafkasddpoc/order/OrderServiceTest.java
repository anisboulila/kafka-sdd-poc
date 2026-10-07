package com.example.kafkasddpoc.order;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    private static final String CREATED_AT = "2026-10-05T16:00:00Z";
    // Short bound so the timeout test is fast; production value comes from app.orders.publication-timeout.
    private static final Duration TIMEOUT = Duration.ofMillis(300);

    @Mock
    private KafkaTemplate<String, OrderCreated> kafkaTemplate;

    @Test
    void waitsForKafkaConfirmationBeforeReturningCreatedEvent() throws Exception {
        Clock clock = Clock.fixed(Instant.parse(CREATED_AT), ZoneOffset.UTC);
        OrderService orderService = new OrderService(kafkaTemplate, clock, TIMEOUT);
        CompletableFuture<SendResult<String, OrderCreated>> publication = new CompletableFuture<>();
        when(kafkaTemplate.send(eq("order-events"), anyString(), any(OrderCreated.class)))
                .thenReturn(publication);

        CompletableFuture<OrderCreated> order = CompletableFuture.supplyAsync(
                () -> orderService.createOrder(
                        new CreateOrderRequest("customer-456", new BigDecimal("19.95"))));

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<OrderCreated> event = ArgumentCaptor.forClass(OrderCreated.class);
        verify(kafkaTemplate, timeout(1000))
                .send(eq("order-events"), key.capture(), event.capture());

        assertFalse(order.isDone());
        assertTrue(key.getValue().matches("[0-9a-fA-F-]{36}"));
        assertEquals(key.getValue(), event.getValue().orderId());
        assertEquals("customer-456", event.getValue().customerId());
        assertEquals(new BigDecimal("19.95"), event.getValue().amount());
        assertEquals(CREATED_AT, event.getValue().createdAt());

        publication.complete(null);
        OrderCreated createdOrder = order.get(1, TimeUnit.SECONDS);

        assertEquals(key.getValue(), createdOrder.orderId());
        assertEquals("customer-456", createdOrder.customerId());
        assertEquals(new BigDecimal("19.95"), createdOrder.amount());
        assertEquals(CREATED_AT, createdOrder.createdAt());
    }

    @Test
    void propagatesExceptionalKafkaPublication() {
        OrderService orderService = new OrderService(kafkaTemplate, Clock.systemUTC(), TIMEOUT);
        CompletableFuture<SendResult<String, OrderCreated>> publication = new CompletableFuture<>();
        publication.completeExceptionally(new KafkaException("Kafka unavailable"));
        when(kafkaTemplate.send(eq("order-events"), anyString(), any(OrderCreated.class)))
                .thenReturn(publication);

        assertThrows(
                OrderPublicationException.class,
                () -> orderService.createOrder(
                        new CreateOrderRequest("customer-456", new BigDecimal("19.95"))));
    }

    @Test
    void failsWithPublicationExceptionWhenKafkaDoesNotConfirmInTime() {
        OrderService orderService = new OrderService(kafkaTemplate, Clock.systemUTC(), TIMEOUT);
        // A future that never completes simulates a broker that has not acknowledged yet:
        // the record is "sent" (stage 1) but never "confirmed" (stage 2).
        when(kafkaTemplate.send(eq("order-events"), anyString(), any(OrderCreated.class)))
                .thenReturn(new CompletableFuture<>());

        long start = System.nanoTime();
        OrderPublicationException failure = assertThrows(
                OrderPublicationException.class,
                () -> orderService.createOrder(
                        new CreateOrderRequest("customer-456", new BigDecimal("19.95"))));

        // The wait is bounded by the configured timeout, and the cause is the timeout itself.
        assertTrue(failure.getCause() instanceof TimeoutException);
        assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() < 5_000);
    }

    @Test
    void wrapsSynchronousKafkaPublicationFailure() {
        OrderService orderService = new OrderService(kafkaTemplate, Clock.systemUTC(), TIMEOUT);
        when(kafkaTemplate.send(eq("order-events"), anyString(), any(OrderCreated.class)))
                .thenThrow(new KafkaException("Kafka unavailable"));

        assertThrows(
                OrderPublicationException.class,
                () -> orderService.createOrder(
                        new CreateOrderRequest("customer-456", new BigDecimal("19.95"))));
    }

    @Test
    void restoresInterruptAndPropagatesInterruptedPublication() {
        OrderService orderService = new OrderService(kafkaTemplate, Clock.systemUTC(), TIMEOUT);
        when(kafkaTemplate.send(eq("order-events"), anyString(), any(OrderCreated.class)))
                .thenReturn(new CompletableFuture<>());

        Thread.currentThread().interrupt();
        try {
            assertThrows(
                    OrderPublicationException.class,
                    () -> orderService.createOrder(
                            new CreateOrderRequest("customer-456", new BigDecimal("19.95"))));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}
