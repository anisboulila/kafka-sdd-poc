package com.example.kafkasddpoc.order;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private static final String ORDER_EVENTS_TOPIC = "order-events";

    private final KafkaTemplate<String, OrderCreated> kafkaTemplate;
    private final Clock clock;

    @Autowired
    public OrderService(KafkaTemplate<String, OrderCreated> kafkaTemplate) {
        this(kafkaTemplate, Clock.systemUTC());
    }

    OrderService(KafkaTemplate<String, OrderCreated> kafkaTemplate, Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.clock = clock;
    }

    public OrderCreated createOrder(CreateOrderRequest request) {
        OrderCreated event = new OrderCreated(
                UUID.randomUUID().toString(),
                request.customerId(),
                request.amount(),
                Instant.now(clock).toString());

        try {
            kafkaTemplate.send(ORDER_EVENTS_TOPIC, event.orderId(), event).get();
            return event;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OrderPublicationException("Interrupted while publishing order event", e);
        } catch (ExecutionException e) {
            throw new OrderPublicationException("Could not publish order event", e);
        }
    }
}
