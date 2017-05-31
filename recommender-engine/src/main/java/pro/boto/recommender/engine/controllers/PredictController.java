package pro.boto.recommender.engine.controllers;


import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import pro.boto.protolang.json.ProtoClient;
import pro.boto.recommender.engine.manager.ProductManager;
import pro.boto.recommender.engine.manager.domain.Product;
import pro.boto.recommender.engine.drill.ProductConnector;
import pro.boto.recommender.engine.manager.PredictManager;

import java.util.List;

@Controller
@RequestMapping("/predict")
public class PredictController {

    @Autowired
    private PredictManager predictManager;

    private enum Level {freguesia,concelho,districto}

    @ApiOperation(value="Get prediction by customer")
    @RequestMapping(path = "/{userId}", method = RequestMethod.GET,produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Product.class),
            @ApiResponse(code = 417, message = "Exception failed")
    })
    public ResponseEntity<?> predictions(@PathVariable(required = true) String userId) {
        return process(userId, Level.freguesia);
    }

    @ApiOperation(value="Get prediction by customer on zoneLevel")
    @RequestMapping(path = "/{userId}/{zoneLevel}", method = RequestMethod.GET,produces = MediaType.APPLICATION_JSON_VALUE)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Product.class),
            @ApiResponse(code = 417, message = "Exception failed")
    })
    public ResponseEntity<?> predictions(@PathVariable(required = true) String userId,
                                        @PathVariable(required=true) Level zoneLevel) {
        return process(userId, zoneLevel);
    }

    private ResponseEntity<?> process(String userId, Level level) {
        try {
            List<Product> products = predictManager.calculate(userId,level.name());
            return new ResponseEntity<List<Product>>(products, HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
            CustomError error = new CustomError("An error has occured");
            return new ResponseEntity<CustomError>(error, HttpStatus.EXPECTATION_FAILED);
        }
    }




}