package pro.boto.recommender.configuration;

public interface KuduConfig {
    String master();
    Integer replicas();
    String actionsTable();
    String ratingsTable();
    String predictionsTable();
}