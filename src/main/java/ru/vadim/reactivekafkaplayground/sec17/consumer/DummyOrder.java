package ru.vadim.reactivekafkaplayground.sec17.consumer;

public record DummyOrder(
        String orderId,
        String customerId
) {
}
