package ru.vadim.reactivekafkaplayground.sec01;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/*
1. Потребление с нескольких топиков, .subscription(Pattern.compile("order.*")); можент принимать паттерн или лист с несколькими топиками

AI Assistant
Этот код демонстрирует потребителя Kafka, который использует библиотеку **Reactor Kafka** для реактивного потребления сообщений из топиков Kafka. Вот подробное объяснение каждой части:

### 1. Задание логгера
```java
public static final Logger log = LoggerFactory.getLogger(Lec02KafkaConsumer.class);
```
Здесь создается логгер с помощью SLF4J. Логгер используется для вывода сообщений журнала (например, ключей, значений или ошибок) в консоль или файл.

---

### 2. Конфигурация потребителя Kafka
```java
Map<String, Object> consumerConfig = Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
        ConsumerConfig.GROUP_ID_CONFIG, "inventory-group-123",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
        ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
);
```

Этот блок создает карту настроек Kafka-потребителя:

- **`ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG`**: Указывает адрес брокера Kafka (`localhost:9092`).
- **`ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG` и `ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG`**: Десериализаторы для ключа и значения сообщений (используется `StringDeserializer`).
- **`ConsumerConfig.GROUP_ID_CONFIG`**: Группа потребителей (`inventory-group-123`). Все потребители в одной группе делят сообщения между собой.
- **`ConsumerConfig.AUTO_OFFSET_RESET_CONFIG: "earliest"`**: Если офсет для группы отсутствует, потребление начинается с самого начала топика.
- **`ConsumerConfig.GROUP_INSTANCE_ID_CONFIG: "1"`**: Явно задает уникальный идентификатор экземпляра (static membership). Это минимизирует задержки при пересборке группы.

---

### 3. Создание параметров получателя (ReceiverOptions)
```java
ReceiverOptions<Object, Object> options = ReceiverOptions.create(consumerConfig)
        .subscription(Pattern.compile("order.*"));
```
- **`ReceiverOptions.create(consumerConfig)`**: Создает реактивные настройки для потребления Kafka. Сюда передаются конфигурации, описанные ранее.
- **`.subscription(Pattern.compile("order.*"))`**: Устанавливает подписку на все топики Kafka, имена которых соответствуют регулярному выражению `order.*` (например, `order-create`, `order-update` и т.д.).

---

### 4. Реактивное получение сообщений из Kafka
```java
KafkaReceiver.create(options)
        .receive()
```
- **`KafkaReceiver.create(options)`**: Создает поток данных из Kafka на основе настроек `ReceiverOptions`.
- **`receive()`**: Метод начинает асинхронное получение сообщений из указанных топиков (в данном случае — тех, которые соответствуют `order.*`).

---

### 5. Обработка каждого сообщения
```java
.doOnNext(r -> log.info("topic: {}, key: {}, value: {}", r.topic(), r.key(), r.value()))
```
- **`doOnNext`**: Реактивный оператор для обработки каждого сообщения. Для каждого полученного сообщения вызывается метод, который выводит в лог:
  - Имя топика (`r.topic()`),
  - Ключ (`r.key()`) и
  - Значение (`r.value()`).

---

### 6. Подтверждение обработки сообщения
```java
.doOnNext(r -> r.receiverOffset().acknowledge())
```
- **`r.receiverOffset().acknowledge()`**: После обработки каждого сообщения отправляется явное подтверждение (acknowledgment) в Kafka, что сообщение было успешно обработано.
- Это необходимо, так как:
  1. В конфиге (по умолчанию) отключена автоматическая фиксация офсетов (`enable.auto.commit=false`).
  2. Kafka сохраняет офсет последнего подтвержденного сообщения, чтобы потребитель продолжал с него в следующий раз.

> **Замечание**: Если офсеты не будут подтверждены, Kafka будет начинать чтение сообщений с начала или с позиции, заданной в конфигурации (`earliest` или `latest`).

---

### 7. Подписка на поток
```java
.subscribe();
```
- **`subscribe()`**: Подписывает потребителя на поток сообщений и запускает обработку.

Сообщения начнут поступать в поток `KafkaReceiver`, а обработка и подтверждение каждого сообщения будут выполнены в порядке, определенном выше.

---

### Пример работы программы:
- Консоль будет отображать что-то вроде следующего (в случае получения сообщений):
```
INFO - topic: order-created, key: order123, value: {"id":123,"item":"itemName","qty":1}
INFO - topic: order-updated, key: order123, value: {"id":123,"item":"itemName","qty":2}
```

---

### Основные отличия от `LEc01KafkaConsumer`:
1. Вместо фиксированного списка топиков `List.of("order-events")` здесь используется подписка на `Pattern.compile("order.*")` для всех топиков, начинающихся с `order`.
2. Группа потребителя имеет другую настройку (`inventory-group-123`).
3. Реактивная обработка сообщений в целом схожа, но логгирование информации о топике добавлено.

---
### Дополнительные детали из комментариев в первом файле:
- **AUTO_OFFSET_RESET_CONFIG**: Параметр `"earliest"` позволяет получать ранее недоставленные сообщения.
- **GROUP_INSTANCE_ID_CONFIG**: Статическое членство минимизирует задержки при пересборке группы потребителей.
- **Acknowledge вручную**: Это используется, чтобы явно подтвердить обработку сообщения и предотвратить его повторное получение.

---

В итоге `Lec02KafkaConsumer` демонстрирует более гибкую подписку на топики и немного модифицированную логику логирования.
 */
public class Lec02KafkaConsumer {

    public static final Logger log = LoggerFactory.getLogger(Lec02KafkaConsumer.class);

    public static void main(String[] args) {
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "inventory-group-123",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );
        ReceiverOptions<Object, Object> options = ReceiverOptions.create(consumerConfig)
                .subscription(Pattern.compile("order.*"));

        KafkaReceiver.create(options)
                .receive()
//                .take(3) можно ограничить потребление, без него консьюмер будет работать всегда
                .doOnNext(r -> log.info("topic: {}, key: {}, value: {}", r.topic(), r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();
    }
}