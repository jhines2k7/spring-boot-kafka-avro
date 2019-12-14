package com.hines.james;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class SpringBootKafkaAvroApplication {
	@Value("${com.hines.james.topic}")
	private String topicName;

	public static void main(String[] args) {
		SpringApplication.run(SpringBootKafkaAvroApplication.class, args);
	}

	@Bean
	CommandLineRunner init(KafkaTemplate<String, Order> kafkaTemplate) {
		return (args) -> {
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
		};
	}
}
