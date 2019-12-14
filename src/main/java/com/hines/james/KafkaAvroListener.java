package com.hines.james;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaAvroListener {
    @Autowired
    FooService fooService;

    @KafkaListener(topics = "${com.hines.james.topic}", groupId = "${com.hines.james.group-id}")
    public void listen(ConsumerRecord<String, Order> order) {
        log.info("Received record: {}", order);

        int value = fooService.doStuff();

        log.info("fooService.doStuff returns a value of {}", value);
    }
}
