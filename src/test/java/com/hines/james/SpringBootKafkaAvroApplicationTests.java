package com.hines.james;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertThat;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.*;

@RunWith(SpringRunner.class)
@DirtiesContext
@SpringBootTest
@Slf4j
public class SpringBootKafkaAvroApplicationTests {
    private static final String TEST_TOPIC_1 = "test-topic-1";

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, false, TEST_TOPIC_1);

    @Test
    public void testTemplate() throws Exception {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-consumer-group", "false",
                embeddedKafka.getEmbeddedKafka());

        DefaultKafkaConsumerFactory<Integer, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);

        ContainerProperties containerProperties = new ContainerProperties(TEST_TOPIC_1);

        KafkaMessageListenerContainer<Integer, String> listenerContainer =
                new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

        final BlockingQueue<ConsumerRecord<Integer, String>> records = new LinkedBlockingQueue<>();

        listenerContainer.setupMessageListener((MessageListener<Integer, String>) record -> {
            log.info("ConsumerRecord: {}", record.toString());
            records.add(record);
        });

        listenerContainer.setBeanName("embeddedListenerContainer");
        listenerContainer.start();

        ContainerTestUtils.waitForAssignment(listenerContainer, embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic());

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka.getEmbeddedKafka());

        ProducerFactory<Integer, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);

        KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);

        kafkaTemplate.setDefaultTopic(TEST_TOPIC_1);
        kafkaTemplate.sendDefault("foo");

        assertThat(records.poll(1, TimeUnit.SECONDS), hasValue("foo"));

        kafkaTemplate.sendDefault(0, 2, "bar");

        ConsumerRecord<Integer, String> received = records.poll(1, TimeUnit.SECONDS);

        assertThat(received, hasKey(2));
        assertThat(received, hasPartition(0));
        assertThat(received, hasValue("bar"));

        kafkaTemplate.send(TEST_TOPIC_1, 0, 2, "baz");

        received = records.poll(1, TimeUnit.SECONDS);

        assertThat(received, hasKey(2));
        assertThat(received, hasPartition(0));
        assertThat(received, hasValue("baz"));
    }
}
