package pro.boto.recommender.engine.manager;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pro.boto.protolang.json.ProtoClient;
import pro.boto.recommender.engine.drill.HeatmapConnector;
import pro.boto.recommender.engine.drill.ProductConnector;
import pro.boto.recommender.engine.manager.domain.Heatmap;
import pro.boto.recommender.engine.manager.domain.Product;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class HeatManager {

    @Autowired
    private HeatmapConnector heatmapConnector;



    public List<Heatmap> obtainActionsSaved(String start, String end) {
        return heatmapConnector.obtainActionsSaved(start, end);
    }

    public List<Heatmap> obtainActionsContacts(String start, String end) {
        return heatmapConnector.obtainActionsContacts(start, end);
    }

    public List<Heatmap> obtainActionsShares(String start, String end) {
        return heatmapConnector.obtainActionsShares(start, end);
    }

    public List<Heatmap> obtainActionsViews(String start, String end) {
        return heatmapConnector.obtainActionsViews(start, end);
    }


}
