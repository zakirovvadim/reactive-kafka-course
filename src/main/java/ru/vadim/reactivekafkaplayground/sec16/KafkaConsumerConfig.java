package ru.vadim.reactivekafkaplayground.sec16;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ReceiverOptions<String, String> receiverOptions(KafkaProperties kafkaProperties) {
        return ReceiverOptions.<String, String>create(kafkaProperties.buildConsumerProperties())
                .subscription(List.of("order-events"));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, String> consumerTemplate(ReceiverOptions<String, String> receiverOptions) {
        return new ReactiveKafkaConsumerTemplate<>(receiverOptions);
    }
}
