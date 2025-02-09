package ru.vadim.reactivekafkaplayground.sec06;

// при добавлении или удалении консьюмеров и создании 3 партиций, можно увидеть как партиции переназначаются работающим консьюмерам
/*
При 1.
    - **`CooperativeStickyAssignor.class`**
партиции назначаются как
с1 - p1, p2, p3

с1 - p1, p2
+ c2 - p3

с1 - p1
+ c2 - p3
+ c3 - p2

Т.е. мы у первого консьюмера забрали лишннюю партицию
При стандартном поведении будет так
с1 - p1, p2, p3

с1 - p1, p2
+ c2 - p2

с1 - p1
+ c2 - p3
+ c3 - p2
т.е. номер спускается как бы вниз, при этом приходится забирать все сообщения и передавать новому консьюмеру
 */
public class KafkaConsumerGroup {

    private static class Consumer1 {
        public static void main(String[] args) {
            KafkaConsumer.start("1");
        }
    }
    private static class Consumer2 {
        public static void main(String[] args) {
            KafkaConsumer.start("2");
        }
    }
    private static class Consumer3 {
        public static void main(String[] args) {
            KafkaConsumer.start("3");
        }
    }


}
