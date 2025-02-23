package ru.vadim.reactivekafkaplayground.sec15;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.List;
import java.util.Map;
/*
### Конспект лекции о Kafka Transactions

#### Основные тезисы:

1. **Kafka Transactions:**
   - Kafka поддерживает транзакции, позволяя приложениям выполнять обработку сообщений "точно один раз" *(exactly-once semantics)*.
   - Эта концепция транзакций лежит в основе Kafka Streams.

2. **Основные элементы:**
   - **Producer (производитель):** приложение, отправляющее сообщения в Kafka.
   - **Kafka Broker:** часть Kafka, которая взаимодействует с producent и сохраняет данные на топики.
   - **Transaction Coordinator:** компонент Kafka, управляющий транзакцией.
     - Координатор отслеживает состояние транзакций и записывает их в **топик состояния транзакций** (internal topic `transaction-state`).
   - **Consumers (потребители):**
     - Один потребитель настроен на чтение *commit*-сообщений.
     - Другой потребитель может считывать как *commit*, так и `uncommitted` сообщения.

3. **Работа транзакции:**
   - **Инициализация:**
     - Producer настраивается для транзакционной работы с помощью параметра `transactional.id`.
     - Producer связывается с координатором транзакций, чтобы получить подтверждение на начало транзакции.
   - **Процесс отправки сообщений:**
     - Producer отправляет сообщения на топик (они находятся в статусе `pending` или ожидающего подтверждения).
     - Координатор отмечает их состояние.
   - **Коммит или откат:**
     - Если транзакция завершается успешно, Producer выполняет коммит (`commit`), и сообщения становятся видимыми для потребителей в режиме `read_committed`.
     - В случае отмены транзакции (`abort`) сообщения остаются в Kafka, но помечаются как "отменённые" (они не удаляются, так как Kafka поддерживает только добавление данных `append-only`).

4. **Чтение сообщений потребителями (`consumers`):**
   - **Потребитель с `read_committed`:**
     - Видит только закоммиченные сообщения. Отклонённые или ожидающие транзакции сообщения игнорируются.
   - **Потребитель с `read_uncommitted`:**
     - Видит все сообщения, включая некоммиченные и отклонённые.

5. **Особенности работы:**
   - Если Producer начал транзакцию, написал сообщения, но так и не закоммитил её, потребители с `read_committed` не видят этих сообщений.
   - Отменённые сообщения никогда не удаляются, но остаются помеченными как "aborted".
   - Kafka-партиции предоставляют только операции добавления (`append-only`) и не поддерживают изменения данных.

6. **Режимы для потребителей:**
   - **`read_committed`:**
     - Рекомендуется, если Producer работает в транзакционном режиме и вы хотите обрабатывать только завершённые транзакции.
   - **`read_uncommitted`:**
     - Используется, если вас устраивает чтение любых сообщений (например, для отладки или анализа).

7. **Ограничения транзакций:**
   - Гарантия "точно один раз" относится только к процессам внутри Kafka (отправка/чтение сообщений).
   - Если приложение обрабатывает сообщения с вызовами к сторонним сервисам или базам данных, Kafka не может гарантировать "exactly-once" семантику для всей системы.

8. **Производительность:**
   - Использование транзакций в Kafka увеличивает нагрузку и снижает производительность (небольшой "overhead").

#### Основное замечание:
Kafka транзакции добавлены для обеспечения семантики *exactly-once* только в рамках Kafka тем и операций. Если приложение взаимодействует с внешними системами, разработчики должны самостоятельно гарантировать атомарность операций или учитывать возможные проблемы.
 */
public class TransferDemo {

    public static final Logger log = LoggerFactory.getLogger(TransferDemo.class);

    public static void main(String[] args) {
        var transferEventConsumer = new TransferEventConsumer(kafkaReceiver());
        var transferEventProcessor = new TransferEventProcessor(kafkaSender());

        transferEventConsumer
                .receiver()
                .transform(transferEventProcessor::process)
                .doOnNext(r -> log.info("transfer success: {}", r.correlationMetadata()))
                .doOnError(e -> log.error("transfer error: {}", e.getMessage()))
                .subscribe();
    }

    private static KafkaReceiver<String, String> kafkaReceiver() {
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );
        var options = ReceiverOptions.<String, String>create(consumerConfig)
                .subscription(List.of("transfer-requests"));

        return KafkaReceiver.create(options);
    }

    private static KafkaSender<String, String> kafkaSender() {
        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "money-transfer"
        );
        var options = SenderOptions.<String, String>create(producerConfig);
        return KafkaSender.create(options);
    }
}
