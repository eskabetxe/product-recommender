package pro.boto.recommender.configuration;

public interface KafkaConfig {
    String host();
    String topic();
    String groupId();
    String acks();
}
