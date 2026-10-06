package com.example.kafkasddpoc.order;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    @Autowired
    public OrderService(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        this(kafkaTemplate, objectMapper, Clock.systemUTC());
    }

    OrderService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper, Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    public OrderCreated createOrder(CreateOrderRequest request) {
        OrderCreated event = new OrderCreated(
                UUID.randomUUID().toString(),
                request.customerId(),
                request.amount(),
                Instant.now(clock).toString());

        try {
            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(ORDER_EVENTS_TOPIC, event.orderId(), payload).get();
            return event;
        } catch (JsonProcessingException e) {
            throw new OrderPublicationException("Could not serialize order event", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OrderPublicationException("Interrupted while publishing order event", e);
        } catch (ExecutionException e) {
            throw new OrderPublicationException("Could not publish order event", e);
        }
    }
}
