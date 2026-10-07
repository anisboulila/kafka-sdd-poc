package com.example.kafkasddpoc.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    @KafkaListener(
            id = "paymentConsumerListener",
            topics = "order-events",
            groupId = "payment-group")
    public void consume(OrderCreated event) {
        log.info("Simulating payment for orderId={} amount={}", event.orderId(), event.amount());
    }
}
