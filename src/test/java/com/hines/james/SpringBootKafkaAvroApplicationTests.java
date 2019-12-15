package com.hines.james;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringBootKafkaAvroApplicationTests {
    @Value("${com.hines.james.topic}")
    private String topicName;

    @Autowired
    KafkaTemplate<String, Order> kafkaTemplate;

    @Autowired
    FooService fooService;

    @Test
    public void listen_calls_fooService_doStuff_and_returns_a_value_of_10() throws Exception {
        Order order = Order.newBuilder()
                .setOrderId("654321")
                .setCustomerId("123456")
                .setSupplierId("aabbccdd")
                .setFirstName("James")
                .setLastName("Hines")
                .setItems(5)
                .setPrice(10.00f)
                .setWeight(5.00f)
                .setAutomatedEmail(true)
                .build();

        kafkaTemplate.send(topicName, 0, (String)order.getOrderId(), order);

        Thread.sleep(1000);

        assertThat(fooService.doStuff()).isEqualTo(10);
    }
}
