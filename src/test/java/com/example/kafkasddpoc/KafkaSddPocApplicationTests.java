package com.example.kafkasddpoc;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
class KafkaSddPocApplicationTests {

    @Test
    void contextLoads() {
    }
}
