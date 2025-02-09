package ru.vadim.reactivekafkaplayground.sec06;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

/*
пример ребалансировки партиций

1.
    - **`CooperativeStickyAssignor.class`**
Это класс из Kafka, который реализует **"кооперативный липкий" (Cooperative Sticky)**
алгоритм распределения партиций. Этот алгоритм пытается минимизировать перераспределение партиций между потребителями
при изменениях в группе (например, при добавлении или удалении нового потребителя)
 */
public class KafkaConsumer {

    public static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void start(String instanceId) {
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId,
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getClasses()
        );
        ReceiverOptions<Object, Object> options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events"));

        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();
    }

}
