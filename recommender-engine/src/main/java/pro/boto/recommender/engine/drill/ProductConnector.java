package pro.boto.recommender.engine.drill;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import pro.boto.recommender.engine.manager.domain.Product;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Service
public class ProductConnector extends DrillConnector {

    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final static String QUERY_PRODUCTS =
            "  select p.* " +
            "  from kudu.products p ";
    public List<Product> obtainProducts()  {
        try {
            ResultSet rs = executeQuery(QUERY_PRODUCTS);
            return mapProducts(rs);
        } catch (SQLException e) {
            LOG.error("error obtaining products. "+ e.getLocalizedMessage(), e);
            return new ArrayList<>();
        }
    }

    private final static String QUERY_PRODUCTS_WITH_POSITIVE_ACTIONS =
            " select t.* " +
            " from kudu.products t " +
            " where t.productid in ( " +
            "  select distinct r.productid " +
            "  from kudu.userevents r " +
            "  where r.action in ('PROPERTY_SAVED','PROPERTY_CONTACTED','PROPERTY_SHARED') " +
            "    and r.userid='%s' " +
            " ) ";
    public List<Product> obtainPositiveReactions(String userId)  {
        try {
            ResultSet rs = executeQuery(String.format(QUERY_PRODUCTS_WITH_POSITIVE_ACTIONS, userId));
            return mapProducts(rs);
        } catch (SQLException e) {
            LOG.error("error obtaining ratings. "+ e.getLocalizedMessage(), e);
            return new ArrayList<>();
        }
    }

    private final static String QUERY_PRODUCTS_PREDICT_BY_USER =
            " select t.* " +
            " from kudu.products t " +
            " where t.productid not in ( " +
            "  select distinct r.productid " +
            "  from kudu.userevents r " +
            "  where r.userid='%s' " +
            " ) ";
    private final static String QUERY_PRODUCTS_PREDICT_LIMIT =
            " and %s in ( " +
            "  select p.%s " +
            "  from kudu.useractions r " +
            "  inner join kudu.products p on p.productid=r.productid  " +
            "  where r.userid = '%s'  " +
            "  group by p.%s " +
            " ) ";
    public List<Product> obtainToPredict(String userId, String limit)  {
        try {
            String query = String.format(QUERY_PRODUCTS_PREDICT_BY_USER,userId);
            if("freguesia".equalsIgnoreCase(limit)
                    || "concelho".equalsIgnoreCase(limit)
                    || "districto".equalsIgnoreCase(limit)){
                query += String.format(QUERY_PRODUCTS_PREDICT_LIMIT,limit,limit,userId,limit);
            }

            ResultSet rs = executeQuery(query);
            return mapProducts(rs);
        } catch (SQLException e) {
            LOG.error("error obtaining predicts. "+ e.getLocalizedMessage(), e);
            return new ArrayList<>();
        }
    }

    private List<Product> mapProducts(ResultSet rs) throws SQLException{
        List<Product> values = new ArrayList<>();
        while(rs.next()){
            Product product = new Product();
            product.productId = rs.getLong("productId");
            product.districto = rs.getString("districto");
            product.concelho = rs.getString("concelho");
            product.freguesia = rs.getString("freguesia");
            product.latitude = rs.getDouble("latitude");
            product.longitude = rs.getDouble("longitude");
            product.typology = rs.getString("typology");
            product.operation = rs.getString("operation");
            product.condiction = rs.getString("condiction");
            product.bedrooms = rs.getInt("bedrooms");
            product.bathrooms = rs.getInt("bathrooms");
            product.contructedArea = rs.getInt("contructedArea");
            product.plotArea = rs.getInt("plotArea");
            product.elevator = rs.getBoolean("elevator");
            product.parking = rs.getBoolean("parking");
            values.add(product);
        }
        return values;
    }
}
