package pro.boto.recommender.aquisiction.provider.storm;


import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import pro.boto.recommender.aquisiction.provider.storm.bolts.*;
import pro.boto.recommender.configuration.*;
import pro.boto.recommender.configuration.KafkaConfig;

import java.io.IOException;

public class Topology {

    public final static StormTopology createTopology(String drpcName) throws IOException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("tracker", obtainKafkaSpout(),5);
        builder.setBolt("eventsave", new EventSaveBolt(), 50).shuffleGrouping("tracker");
        builder.setBolt("action", new ActionCalculatorBolt(), 10).shuffleGrouping("eventsave");
        builder.setBolt("actionsave", new ActionSaveBolt(), 10).shuffleGrouping("action");

        return builder.createTopology();

    }

    private static KafkaSpout obtainKafkaSpout(){
        KafkaConfig kafkaConfig = RecommenderConfig.obtainKafkaConfig();
        ZookeeperConfig zookeeperConfig = RecommenderConfig.obtainZookeeperConfig();
        BrokerHosts hosts = new ZkHosts(zookeeperConfig.master());

        SpoutConfig config = new SpoutConfig(hosts, kafkaConfig.topic(), "/"+ kafkaConfig.topic(), kafkaConfig.groupId());
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
		config.ignoreZkOffsets = true;

        KafkaSpout kafka = new KafkaSpout(config);

        return kafka;
    }
}
