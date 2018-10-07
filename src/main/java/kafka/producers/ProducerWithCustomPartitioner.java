package kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerWithCustomPartitioner implements KafkProducer {

    private SimpleKafkaProducer origin;

    public ProducerWithCustomPartitioner(SimpleKafkaProducer originl) {
        this.origin = originl;
    }

    @Override
    public void run(KafkaProducer<String, String> producer) {
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("messages", Integer.toString(i),
                    Integer.toString(i)), (metadata, exception) -> System.out.println("Topic: " + metadata.topic() +
                    " offset: " + metadata.offset() +
                    " partition #: " + metadata.partition() +
                    " timestamp: " + metadata.timestamp()));
        }
        producer.close();
    }

    @Override
    public KafkaProducer<String, String> initialize() {
        return origin.initialize();
    }

}
