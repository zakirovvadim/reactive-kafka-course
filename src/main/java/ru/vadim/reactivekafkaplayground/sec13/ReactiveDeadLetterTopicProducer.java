package ru.vadim.reactivekafkaplayground.sec13;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.util.retry.RetryBackoffSpec;
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
    private final RetryBackoffSpec retrySpec;

    public ReactiveDeadLetterTopicProducer(KafkaSender<K, V> sender, RetryBackoffSpec retrySpec) {
        this.sender = sender;
        this.retrySpec = retrySpec;
    }

    public Mono<SenderResult<K>> produce(ReceiverRecord<K, V> record) {
        var sr = toSenderRecord(record);
        return this.sender.send(Mono.just(sr)).next();
    }

    private SenderRecord<K, V, K> toSenderRecord(ReceiverRecord<K, V> record) {
        var pr = new ProducerRecord<>(
                record.topic() + "-dlt",
                record.key(),
                record.value()
        );
        return SenderRecord.create(pr, record.key());
    }


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
