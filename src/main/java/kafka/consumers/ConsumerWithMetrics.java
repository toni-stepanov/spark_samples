package kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

public class ConsumerWithMetrics implements Consumer{

    private final static Logger logger = Logger.getLogger(ConsumerWithMetrics.class);
    private DebugConsumer origin;

    public ConsumerWithMetrics(DebugConsumer debugConsumer) {
        this.origin = debugConsumer;
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void run(KafkaConsumer<String, String> kafkaConsumer) {
        while (true) {
            ConsumerRecords<String, String> records = records(kafkaConsumer);
            if (!records.isEmpty()) {
                kafkaConsumer.metrics().forEach((k, v) -> logger.info(k + " " + v.value()));
            }
            print(records);
        }
    }

    @Override
    public ConsumerRecords<String, String> records(KafkaConsumer<String, String> kafkaConsumer) {
        return origin.records(kafkaConsumer);
    }

    @Override
    public void print(ConsumerRecords<String, String> records) {
        origin.print(records);
    }

    @Override
    public KafkaConsumer<String, String> get() {
        return origin.get();
    }
}
