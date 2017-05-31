package pro.boto.recommender.configuration;

public class RecommenderConfig {

    private static Configurations configurations = Configurations.fromFiles("recommender-application.yaml");

    private final static String CONFIG_ZOOKEEPER = "zookeeper";
    private final static String CONFIG_KAFKA = "kafka";
    private final static String CONFIG_KUDU = "kudu";
    private final static String CONFIG_DRILL = "drill";
    private final static String CONFIG_HDFS = "hdfs";

    public static ZookeeperConfig obtainZookeeperConfig(){
        return configurations.bind(CONFIG_ZOOKEEPER, ZookeeperConfig.class);
    }

    public static HdfsConfig obtainHdfsConfig(){
        return configurations.bind(CONFIG_HDFS, HdfsConfig.class);
    }

    public static KafkaConfig obtainKafkaConfig(){
        return configurations.bind(CONFIG_KAFKA, KafkaConfig.class);
    }

    public static KuduConfig obtainKuduConfig(){
        return configurations.bind(CONFIG_KUDU, KuduConfig.class);
    }

    public static DrillConfig obtainDrillConfig(){
        return configurations.bind(CONFIG_DRILL, DrillConfig.class);
    }

}
