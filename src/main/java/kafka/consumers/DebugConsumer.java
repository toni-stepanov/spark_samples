package kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Properties;

public class DebugConsumer implements Consumer {

    private final static Logger logger = Logger.getLogger(DebugConsumer.class);

    @SuppressWarnings("InfiniteLoopStatement")
    public void run(KafkaConsumer<String, String> kafkaConsumer) {
        while (true) {
            print(records(kafkaConsumer));
        }
    }

    @Override
    public ConsumerRecords<String, String> records(KafkaConsumer<String, String> kafkaConsumer) {
        return kafkaConsumer.poll(100);
    }

    @Override
    public void print(ConsumerRecords<String, String> records) {
        records.forEach(e ->
                logger.info("Offset " + e.offset() + " key= " + e.key() + " value= " + e.value()));
    }

    @Override
    public KafkaConsumer<String, String> get() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("session.timeout", 10000);
        props.put("group.id", "test");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("messages"));
        return consumer;
    }

}
