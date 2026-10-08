package com.example.kafkasddpoc.order;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class OrderKafkaConfiguration {

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name("order-events")
                .partitions(1)
                .replicas(1)
                .build();
    }

    // Dead Letter Topic, declared explicitly instead of relying on auto-creation.
    // DeadLetterPublishingRecoverer sends a failed record to the SAME partition number as the
    // source, so this topic must always have at least as many partitions as order-events.
    @Bean
    public NewTopic orderEventsDeadLetterTopic() {
        return TopicBuilder.name("order-events.DLT")
                .partitions(1)
                .replicas(1)
                .build();
    }

    // Container factory used ONLY by PaymentConsumer. Spring Boot's default factory
    // ("kafkaListenerContainerFactory") stays untouched, so Notification keeps its own behavior
    // and is not affected by Payment failures.
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> paymentKafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaTemplate<String, OrderCreated> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);

        // CONSUMER-side retry (not to be confused with the producer's `retries` of task 1.2):
        // the container re-delivers the same record to the listener after a failure.
        // FixedBackOff(1000, 3) = wait 1 s between attempts, 3 retries => 4 deliveries in total.
        // The retry is blocking: this partition makes no progress while retrying.
        // The destination is explicit: Spring Kafka 3.3's default suffix is "-dlt", which would not\r\n        // match our order-events.DLT. Same partition number as the source record (see topic comment).\r\n        // Once retries are exhausted the recoverer publishes the record (same key and value, plus
        // headers describing the failure) to order-events.DLT, then the offset moves on.
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) ->
                        new TopicPartition("order-events.DLT", record.partition())),
                new FixedBackOff(1000L, 3L));
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}