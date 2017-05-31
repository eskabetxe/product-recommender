package pro.boto.recommender.engine.manager;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pro.boto.recommender.engine.manager.domain.Product;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class PredictManager {

    private final String USER = "userId";
    private final String PRODUCT = "productId";
    private final String PREDICTION = "prediction";
    private final Integer PREDICITON_MIN = 3;

    private PipelineModel model;
    private StructType struct;


    @Autowired
    private ProductManager productManager;


    public PredictManager(){
        model = SparkConfig.obtainModel();
        struct = new StructType()
                .add(USER, DataTypes.LongType)
                .add(PRODUCT, DataTypes.LongType);
    }


    public List<Product> calculate(String customer, String level) {
        List<Product> toPredict = productManager.obtainToPredict(customer, level);
        List<Product> predicted = predict(customer, toPredict);
        return predicted;
    }

    private List<Product> predict(String customerId, List<Product> products) {
        List<Row> tuples = products.stream()
                .map(row -> obtainRow(customerId,row.productId))
                .collect(Collectors.toList());

        List<Long> predict = model
                .transform(SparkConfig.obtainDataFrame(tuples, struct))
                .na().drop()
                .where(new Column(PREDICTION).geq(PREDICITON_MIN))
                .orderBy(new Column(PREDICTION).desc())
                //   .limit(100)
                .toJavaRDD()
                .map(row -> row.getLong(1))
                .collect()
                ;

        return products.stream()
                .filter(t -> predict.contains(t.productId))
                .collect(Collectors.toList());
    }

    private Long map(Row row){
        return (Long)row.getAs(PRODUCT);
    }


    private Row obtainRow(String customerId, Long productId){
        Long ccID = new Long(customerId.replace("U","").replace("C",""));
        return RowFactory.create(ccID, productId);
    }




}
