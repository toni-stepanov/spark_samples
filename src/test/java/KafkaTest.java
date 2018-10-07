import kafka.consumers.DebugConsumer;
import kafka.consumers.ConsumerWithMetrics;
import kafka.producers.ProducerWithMetadata;
import kafka.producers.SimpleKafkaProducer;
import org.junit.jupiter.api.Test;

class KafkaTest {

    @Test
    void simpleProducer() throws InterruptedException {
        ConsumerWithMetrics consumerWithMetrics = new ConsumerWithMetrics(new DebugConsumer());
        consumerWithMetrics.run(consumerWithMetrics.get());
        Thread.sleep(3000L);
        SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer();
        simpleKafkaProducer.run(simpleKafkaProducer.initialize());
    }

    @Test
    void metadataProducer() throws InterruptedException {
        ConsumerWithMetrics consumerWithMetrics = new ConsumerWithMetrics(new DebugConsumer());
        consumerWithMetrics.run(consumerWithMetrics.get());
        Thread.sleep(3000L);
        ProducerWithMetadata producer = new ProducerWithMetadata(new SimpleKafkaProducer());
        producer.run(producer.initialize());
    }

}
