package pro.boto.recommender.ingestion.simulator;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import pro.boto.recommender.ingestion.IngestConfig;

public class IngestSimulator {

    public static void main(String[] args) {
        IngestSimulator.start();
    }

    public static void start(){

        IngestConfig.SotConfig sot = IngestConfig.obtainSotConfig();
        IngestProducer producer = new IngestProducer();

        //List<String> days = Arrays.asList("03","04");

        List<String> days = Arrays.asList("03","04","05","06","07","08","09",
                 "10","11","12","13","14","15","16","17","18","19",
                 "20","21");
        //,"22","23","24","25"
       // List<String> days = Arrays.asList("10","11","12","13","14","15","16","17","18","19","20");
       // List<String> days = Arrays.asList("10");
        days.forEach(day -> {
            try {
                String file = sot.basepath()+day+sot.filename();
                FileUtils.readLines(new File(file), sot.charset())
                        .parallelStream()
                        .forEach(line -> producer.produce(line));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}
