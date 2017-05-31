package pro.boto.recommender.engine.manager;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class SparkConfig {

    private static SparkSession sparkSession = SparkSession.builder()
            .appName("recommender-engine")
            .master("local[*]")
            .getOrCreate();


    public static SparkSession obtainSession(){
        return sparkSession;
    }

    public static Dataset<Row> obtainDataFrame(List<Row> tuples, StructType type){
        SparkSession sparkSession = obtainSession();

        return sparkSession
                .createDataFrame(tuples, type);

    }
    public static PipelineModel obtainModel(){
        SparkSession spark = obtainSession();
        return PipelineModel.load("/home/boto/tmp/model/save");
    }

}
