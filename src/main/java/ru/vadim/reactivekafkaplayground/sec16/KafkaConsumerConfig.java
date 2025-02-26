package ru.vadim.reactivekafkaplayground.sec16;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
/*
Ошибка при потреблении данных означает
Caused by: java.lang.IllegalArgumentException: The class 'ru.vadim.reactivekafkaplayground.sec16.OrderEvent' is not in the trusted packages: [java.util, java.lang]. If you believe this class is safe to deserialize, please provide its name. If the serialization is only done by a trusted source, you can also enable trust all (*).
1. **Как работает сериализация/десериализация в Spring Kafka:**
При публикации событий продюсер добавляет специальные заголовки (headers) к каждому сообщению, одним из таких заголовков является `__TypeId__`:
    - Этот заголовок содержит полностью квалифицированное имя класса (например: `ru.vadim.reactivekafkaplayground.sec16.OrderEvent`), который нужно будет десериализовать на стороне consumer.
    - Это позволяет поддерживать полиморфизм. Например, вы можете иметь интерфейс `Car` и несколько его реализаций, таких как `BMW` или `Honda`, или интерфейс, связанный с несколькими событиями: `OrderCreatedEvent`, `OrderUpdatedEvent` и т.д.

2. **Зачем используется trusted-пакет:**
    - В целях безопасности Spring Kafka требует явно указать "доверенные пакеты", чтобы быть уверенным, что десериализация выполняется с надёжными классами, определёнными только в известных приложению пакетах.
    - Это защита от атак, где злоумышленник мог бы передать вредоносный объект, который consumer мог бы ошибочно десериализовать, например, в класс с деликатной функциональностью, как "отправить пароль".


Таким образом, нужно вно добавить свой пакет в список доверенных пакетов d ghjgthnb -  "spring.json.trusted.packages": "ru.vadim.reactivekafkaplayground.sec16"
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ReceiverOptions<String, OrderEvent> receiverOptions(KafkaProperties kafkaProperties) {
        return ReceiverOptions.<String, OrderEvent>create(kafkaProperties.buildConsumerProperties())
                .consumerProperty(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS, false) // нужно чтобы посмотреть какой тип приходит с сообщением, так как по умолчанию консьюмер считывает тип и потом его удаляет
                .subscription(List.of("order-events"));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, OrderEvent> consumerTemplate(ReceiverOptions<String, OrderEvent> receiverOptions) {
        return new ReactiveKafkaConsumerTemplate<>(receiverOptions);
    }
}
