package pro.boto.recommender.ingestion;

import pro.boto.recommender.ingestion.simulator.IngestSimulator;
import pro.boto.recommender.ingestion.tables.TableCreation;
import pro.boto.recommender.ingestion.tables.TableProduct;

public class IngestMain {

    public static void main(String[] args) throws Exception {
        TopicCreation.start();

        TableCreation.start();

        TableProduct.charge();

        IngestSimulator.start();
    }
}
