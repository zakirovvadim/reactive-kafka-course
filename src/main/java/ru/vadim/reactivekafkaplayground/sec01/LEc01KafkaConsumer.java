package ru.vadim.reactivekafkaplayground.sec01;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

/*
1. ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" для того чтобы получить все ранние сообщения
2. ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1" // При подключении нового потребителя Kafka может ожидать до 45 секунд таймаута,
прежде чем передаст сообщения новому экземпляру
(при какой-либо потере соединения со старым компонентом).
Явное указание параметра `GROUP_INSTANCE_ID_CONFIG` может избавить от этих задержек.
3. Так как при каждом запуске потребленеи будет происходить снова, это связано с тем, что при подключении консьюмера к брокеру
брокер видит, что end-offset - общее количество сообщений, напршимер 15, и current-offset - текущий оффсет, который равен нулю
так как после послежднего запуска потребитель не отправил подтверждение об обработке - acknowledge, поэтому при работе с полученными
сообщениями нам надо подтверждать их .doOnNext(r -> r.receiverOffset().acknowledge()), желательно после обработки самого сообщения
По умолчанию auto.commit.interval.ms = 5000 acknowledge отправляется с интервалом, чтоб не нагружать сеть.
Также парамтером enable.auto.commit = false в тру можно установить автоматический acknowledge.
 */
public class LEc01KafkaConsumer {

    public static final Logger log = LoggerFactory.getLogger(LEc01KafkaConsumer.class);

    public static void main(String[] args) {
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest", // для того чтобы получить все ранние сообщения
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1" // При подключении нового потребителя Kafka может ожидать до 45 секунд таймаута, прежде чем передаст сообщения новому экземпляру (при какой-либо потере соединения со старым компонентом). Явное указание параметра `GROUP_INSTANCE_ID_CONFIG` может избавить от этих задержек.
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
