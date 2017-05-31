package pro.boto.recommender.engine.manager;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pro.boto.recommender.engine.drill.ProductConnector;
import pro.boto.recommender.engine.manager.domain.Product;

import java.util.List;

@Service
public class ProductManager {

    @Autowired
    private ProductConnector productConnector;



    public List<Product> obtainPositiveReactions(String customer) {
        return productConnector.obtainPositiveReactions(customer);
    }

    public List<Product> obtainProducts() {
        return productConnector.obtainProducts();
    }

    public List<Product> obtainToPredict(String customer, String level) {
        return productConnector.obtainToPredict(customer, level);
    }




}
