package pro.boto.recommender.engine.ui;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import pro.boto.protolang.json.ProtoClient;
import pro.boto.recommender.engine.manager.HeatManager;
import pro.boto.recommender.engine.manager.ProductManager;
import pro.boto.recommender.engine.manager.domain.Heatmap;
import pro.boto.recommender.engine.manager.domain.Product;
import pro.boto.recommender.engine.drill.HeatmapConnector;
import pro.boto.recommender.engine.drill.ProductConnector;
import pro.boto.recommender.engine.manager.PredictManager;
import org.springframework.ui.Model;

import java.util.List;

@Controller
@RequestMapping("/ui")
public class UiController {

    @Autowired
    private PredictManager predictManager;
    @Autowired
    private HeatManager heatManager;
    @Autowired
    private ProductManager productManager;

    @RequestMapping(method = RequestMethod.GET)
    public String ratingsForm(Model model) {
        return "ui-form";
    }

    @RequestMapping(value="/predict", method = RequestMethod.POST)
    public String predict(Model model, UiRequest req) {
        model.addAttribute("ratings", obtainPositiveReactions(req.getCustomer()));
        model.addAttribute("predicts", obtainPredictions(req));
        return "predict-result";
    }


    @RequestMapping(value="/heatmap", method = RequestMethod.POST)
    public String heatmap(Model model, UiRequest req) {
        String start = req.getStartDate();
        String end = req.getEndDate();

        model.addAttribute("saved", obtainActionsSaved(start, end));
        model.addAttribute("contacts", calculateContacts(start, end));
        model.addAttribute("shares", obtainActionsShares(start, end));
        model.addAttribute("views", obtainActionsViews(start, end));
        return "heatmap-result";
    }

    @RequestMapping(value = "/products", method = RequestMethod.POST)
    public String preditions(Model model) {
        model.addAttribute("products", obtainProducts());
        return "product-result";
    }



    private String obtainPredictions(UiRequest req) {
        try {
            List<Product> predicts = predictManager.calculate(req.getCustomer(),req.getLevel());
            return ProtoClient.obtainJson(predicts);
        }catch (Exception e) {
            return "";
        }
    }


    private String obtainActionsSaved(String start, String end) {
        try {
            List<Heatmap> contacts = heatManager.obtainActionsSaved(start, end);
            return ProtoClient.obtainJson(contacts);
        }catch (Exception e) {
            return "";
        }
    }

    private String calculateContacts(String start, String end) {
        try {
            List<Heatmap> contacts = heatManager.obtainActionsContacts(start, end);
            return ProtoClient.obtainJson(contacts);
        }catch (Exception e) {
            return "";
        }
    }

    private String obtainActionsShares(String start, String end) {
        try {
            List<Heatmap> contacts = heatManager.obtainActionsShares(start, end);
            return ProtoClient.obtainJson(contacts);
        }catch (Exception e) {
            return "";
        }
    }

    private String obtainActionsViews(String start, String end) {
        try {
            List<Heatmap> contacts = heatManager.obtainActionsViews(start, end);
            return ProtoClient.obtainJson(contacts);
        }catch (Exception e) {
            return "";
        }
    }



    private String obtainPositiveReactions(String customer) {
        try {
            List<Product> rated = productManager.obtainPositiveReactions(customer);
            return ProtoClient.obtainJson(rated);
        }catch (Exception e) {
            return "";
        }
    }

    private String obtainProducts() {
        try {
            List<Product> products = productManager.obtainProducts();
            return ProtoClient.obtainJson(products);
        }catch (Exception e) {
            return "";
        }
    }



}