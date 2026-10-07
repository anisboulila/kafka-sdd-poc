package com.example.kafkasddpoc.order;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private static final String ORDER_EVENTS_TOPIC = "order-events";

    private final KafkaTemplate<String, OrderCreated> kafkaTemplate;
    private final Clock clock;
    // HTTP-side bound on waiting for Kafka's confirmation. Kept slightly above the producer's
    // delivery.timeout.ms so the producer normally reports its own definitive result first;
    // this bound is the safety net that guarantees the HTTP request never waits forever.
    private final Duration publicationTimeout;

    @Autowired
    public OrderService(
            KafkaTemplate<String, OrderCreated> kafkaTemplate,
            @Value("${app.orders.publication-timeout}") Duration publicationTimeout) {
        this(kafkaTemplate, Clock.systemUTC(), publicationTimeout);
    }

    OrderService(
            KafkaTemplate<String, OrderCreated> kafkaTemplate,
            Clock clock,
            Duration publicationTimeout) {
        this.kafkaTemplate = kafkaTemplate;
        this.clock = clock;
        this.publicationTimeout = publicationTimeout;
    }

    public OrderCreated createOrder(CreateOrderRequest request) {
        OrderCreated event = new OrderCreated(
                UUID.randomUUID().toString(),
                request.customerId(),
                request.amount(),
                Instant.now(clock).toString());

        try {
            // send() only hands the record to the producer (stage 1: accepted). It may itself block
            // up to max.block.ms when metadata is unavailable (e.g. broker down).
            // get(timeout) waits for the broker acknowledgement (stage 2: confirmed by Kafka);
            // only then do we answer 201 (stage 3: HTTP response).
            kafkaTemplate.send(ORDER_EVENTS_TOPIC, event.orderId(), event)
                    .get(publicationTimeout.toMillis(), TimeUnit.MILLISECONDS);
            return event;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OrderPublicationException("Interrupted while publishing order event", e);
        } catch (TimeoutException e) {
            // No confirmation in time: the outcome is unknown (the record may still reach Kafka),
            // so we must not claim success. The existing handler turns this into HTTP 503.
            throw new OrderPublicationException("Kafka did not confirm the order event in time", e);
        } catch (ExecutionException e) {
            throw new OrderPublicationException("Could not publish order event", e);
        } catch (KafkaException e) {
            throw new OrderPublicationException("Could not publish order event", e);
        }
    }
}
