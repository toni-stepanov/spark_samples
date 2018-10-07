package kafka.producers;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerWithMetadata implements KafkProducer {

    private SimpleKafkaProducer origin;

    public ProducerWithMetadata(SimpleKafkaProducer originl) {
        this.origin = originl;
    }

    public KafkaProducer<String, String> initialize() {
        return origin.initialize();
    }

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

}
