package pro.boto.recommender.ingestion;

import pro.boto.recommender.configuration.Configurations;

public class IngestConfig {

    private static Configurations configurations = Configurations.fromFiles("recommender-application.yaml");

    private final static String SOT = "sot";

    public interface SotConfig {
        String basepath();
        String filename();
        String charset();
    }

    public static SotConfig obtainSotConfig(){
        return configurations.bind(SOT, SotConfig.class);
    }


}
