package ru.vadim.reactivekafkaplayground.sec05;
/*при добавлении или удалении консьюмеров и создании 3 партиций, можно увидеть как партиции переназначаются работающим консьюмерам
kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --create --partitions 3

Notifying assignor about the new Assignment(partitions=[order-events-0, order-events-1, order-events-2]) - если запустить один консьюмер, можно увидеть это в логах

Если подключить второй консьюмер
20:06:22.567 [reactive-kafka-demo-group-123-1] INFO  o.a.k.c.c.i.ConsumerCoordinator - [Consumer instanceId=1, clientId=consumer-demo-group-123-1, groupId=demo-group-123] Request joining group due to: group is already rebalancing
20:06:22.568 [reactive-kafka-demo-group-123-1] INFO  o.a.k.c.c.i.ConsumerRebalanceListenerInvoker - [Consumer instanceId=1, clientId=consumer-demo-group-123-1, groupId=demo-group-123] Revoke previously assigned partitions order-events-0, order-events-1, order-events-2
20:06:22.570 [reactive-kafka-demo-group-123-1] INFO  o.a.k.c.c.i.ConsumerCoordinator - [Consumer instanceId=1, clientId=consumer-demo-group-123-1, groupId=demo-group-123] (Re-)joining group
20:06:22.579 [reactive-kafka-demo-group-123-1] INFO  o.a.k.c.c.i.ConsumerCoordinator - [Consumer instanceId=1, clientId=consumer-demo-group-123-1, groupId=demo-group-123] Successfully joined group with generation Generation{generationId=2, memberId='1-c380be48-887a-4336-b7ed-c28ae8883abd', protocol='range'}
20:06:22.580 [reactive-kafka-demo-group-123-1] INFO  o.a.k.c.c.i.ConsumerCoordinator - [Consumer instanceId=1, clientId=consumer-demo-group-123-1, groupId=demo-group-123] Finished assignment for group at generation 2: {2-35258f8e-cfa5-4d8d-9bef-5dfe98680458=Assignment(partitions=[order-events-2]), 1-c380be48-887a-4336-b7ed-c28ae8883abd=Assignment(partitions=[order-events-0, order-events-1])}
20:06:22.585 [reactive-kafka-demo-group-123-1] INFO  o.a.k.c.c.i.ConsumerCoordinator - [Consumer instanceId=1, clientId=consumer-demo-group-123-1, groupId=demo-group-123] Successfully synced group in generation Generation{generationId=2, memberId='1-c380be48-887a-4336-b7ed-c28ae8883abd', protocol='range'}
20:06:22.587 [reactive-kafka-demo-group-123-1] INFO  o.a.k.c.c.i.ConsumerCoordinator - [Consumer instanceId=1, clientId=consumer-demo-group-123-1, groupId=demo-group-123] Notifying assignor about the new Assignment(partitions=[order-events-0, order-events-1])
20:06:22.587 [reactive-kafka-demo-group-123-1] INFO  o.a.k.c.c.i.ConsumerRebalanceListenerInvoker - [Consumer instanceId=1, clientId=consumer-demo-group-123-1, groupId=demo-group-123] Adding newly assigned partitions: order-events-0, order-events-1
20:06:22.591 [reactive-kafka-demo-group-123-1] INFO  o.a.k.c.c.internals.ConsumerUtils - Setting offset for partition order-events-1 to the committed offset FetchPosition{offset=42, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:9092 (id: 1 rack: null)], epoch=0}}
20:06:22.591 [reactive-kafka-demo-group-123-1] INFO  o.a.k.c.c.internals.ConsumerUtils - Setting offset for partition order-events-0 to the committed offset FetchPosition{offset=57, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:9092 (id: 1 rack: null)], epoch=0}}

Видно, что первому досталось две партиции - Notifying assignor about the new Assignment(partitions=[order-events-0, order-events-1]), а другому одна notifying assignor about the new Assignment(partitions=[order-events-2])
Если включить третий консьюмер то у каждого будет по одной партиции

После закртия третьего у первого Notifying assignor about the new Assignment(partitions=[order-events-0, order-events-1])
После закрытия второго у первого снова три партиции
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
