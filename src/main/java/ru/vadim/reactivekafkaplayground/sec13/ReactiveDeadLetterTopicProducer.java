package ru.vadim.reactivekafkaplayground.sec13;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.retry.RetrySpec;

import java.util.function.Function;
/**
 * The ReactiveDeadLetterTopicProducer class provides functionality for producing messages
 * to a Dead Letter Topic (DLT) in a reactive context. This is used to handle messages
 * that cannot be processed successfully by a Kafka consumer. The class integrates with Reactor's
 * reactive programming model and Kafka sender APIs to manage retries and error handling.
 *
 * @param <K> the type of the message key
 * @param <V> the type of the message value
 */ /*
Смысл этого класса, сделать универсальный продьюсер, который будет отправлять в отдельный топик сообщения, которые не смогли обработаться консьюмером
 */
public class ReactiveDeadLetterTopicProducer<K, V> {

    public static final Logger log = org.slf4j.LoggerFactory.getLogger(ReactiveDeadLetterTopicProducer.class);
    private final KafkaSender<K, V> sender;
    private final RetrySpec retrySpec;

    public ReactiveDeadLetterTopicProducer(KafkaSender<K, V> sender, RetrySpec retrySpec) {
        this.sender = sender;
        this.retrySpec = retrySpec;
    }

    /**
     * Produces a message to a Dead Letter Topic (DLT) based on the given ReceiverRecord.
     *
     * This method takes a {@code ReceiverRecord} from Kafka, converts it into a {@code SenderRecord}
     * suitable for publishing to a DLT, and sends it using a configured {@code KafkaSender}.
     *
     * @param record the incoming {@code ReceiverRecord} containing the message key, value, and metadata.
     * @return a {@code Mono<SenderResult<K>>} that represents the result of the send operation.
     */
    public Mono<SenderResult<K>> produce(ReceiverRecord<K, V> record) {
        var sr = toSenderRecord(record);
        return this.sender.send(Mono.just(sr)).next();
    }

    /**
     * Converts a {@link ReceiverRecord} into a {@link SenderRecord} for publishing to a dead-letter topic.
     * The dead-letter topic is determined by appending "-dlt" to the original topic of the receiver record.
     * The key of the original record is used as both the key of the {@link SenderRecord} and its metadata.
     *
     * @param record the {@link ReceiverRecord} to be converted
     * @return a new {@link SenderRecord} encapsulating the converted record data and metadata
     */
    private SenderRecord<K, V, K> toSenderRecord(ReceiverRecord<K, V> record) {
        var pr = new ProducerRecord<>(
                record.topic() + "-dlt",
                record.key(),
                record.value()
        );
        return SenderRecord.create(pr, record.key());
    }

    /**
     * Creates a function that processes a reactive pipeline for error handling during Kafka record processing.
     * This function applies retry logic, logs error messages, converts specific exceptions, and executes
     * custom logic to handle failed records, such as forwarding them to a dead-letter topic.
     *
     * The reactive pipeline:
     * 1. Retries processing the record using the configured retry specification.
     * 2. Transforms exceptions of type {@link RecordProcessingException} to their original cause.
     * 3. Logs error messages for unhandled exceptions.
     * 4. Handles {@link RecordProcessingException} by producing the failed record to a dead-letter topic
     *    and acknowledging its offset.
     *
     * @return A function that takes a {@link Mono} of {@link ReceiverRecord} and returns a {@link Mono<Void>}
     *         representing the completion or failure of the error handling pipeline.
     */
    public Function<Mono<ReceiverRecord<K, V>>, Mono<Void>> recordProcessingErrorHandler() {
        return mono -> mono
                .retryWhen(this.retrySpec)
                .onErrorMap(ex -> ex.getCause() instanceof RecordProcessingException, Throwable::getCause) // Эта часть кода используется в реактивной цепочке обработки ошибок. Она выполняет следующее:1. **Проверяет тип изначального исключения**:- `ex.getCause() instanceof RecordProcessingException` — проверяется, является ли причина (cause) переданного исключения объектом типа `RecordProcessingException`. 2. **Маппинг исключения**:- Если условие выше выполняется, исключение заменяется его "причиной" (`Throwable::getCause`). То есть, вместо обертки в `RecordProcessingException`, реактивная цепочка будет видеть исходное исключение. Для чего это нужно? Когда в процессе обработки сообщения Kafka возникает ошибка, она может быть упакована (или обернута) в пользовательский тип исключения `RecordProcessingException`, чтобы передать дополнительную информацию, например, само сообщение (`ReceiverRecord`), вызвавшее эту ошибку. Однако, эта ошибка может быть вызвана другим внутренним исключением, которое важнее для дальнейшей логики.
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(RecordProcessingException.class, ex -> this.produce(ex.getRecord())
                        .then(Mono.fromRunnable(() -> ex.getRecord().receiverOffset().acknowledge())))
                .then();
    }
}
