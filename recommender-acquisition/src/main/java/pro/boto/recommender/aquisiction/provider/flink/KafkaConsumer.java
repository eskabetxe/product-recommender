package pro.boto.recommender.aquisiction.provider.flink;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import pro.boto.recommender.configuration.KafkaConfig;
import pro.boto.recommender.configuration.RecommenderConfig;

import java.util.Properties;
import java.util.UUID;

class KafkaConsumer {

    public static FlinkKafkaConsumer010<String> obtain(){
        KafkaConfig config = RecommenderConfig.obtainKafkaConfig();

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.host());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<>(config.topic(), new SimpleStringSchema(), properties);

        return kafkaConsumer;
    }
}
