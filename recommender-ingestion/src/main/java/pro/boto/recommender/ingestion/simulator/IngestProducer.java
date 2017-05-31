package pro.boto.recommender.ingestion.simulator;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import pro.boto.recommender.configuration.KafkaConfig;
import pro.boto.recommender.configuration.RecommenderConfig;
import pro.boto.recommender.ingestion.IngestConfig;

import java.util.Properties;
import java.util.concurrent.Future;

public class IngestProducer implements AutoCloseable{

    private final org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
    private final KafkaConfig config;

    public IngestProducer() {
        config = RecommenderConfig.obtainKafkaConfig();
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(producerProperties(config));
    }

    private Properties producerProperties(KafkaConfig config){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.host());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    public Future<RecordMetadata> produce(String value) {
        return producer.send(new ProducerRecord<String, String>(config.topic(), value));
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }
}
