package com.example.kafkasddpoc.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    @KafkaListener(
            id = "notificationConsumerListener",
            topics = "order-events",
            groupId = "notification-group")
    public void consume(OrderCreated event) {
        log.info(
                "Simulating notification for orderId={} customerId={}",
                event.orderId(),
                event.customerId());
    }
}
