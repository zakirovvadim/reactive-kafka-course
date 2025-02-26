package ru.vadim.reactivekafkaplayground.sec16;

public record DummyOrder(
        String orderId,
        String customerId
) {
}
