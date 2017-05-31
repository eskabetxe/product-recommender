package pro.boto.recommender.engine.manager.domain;

import pro.boto.protolang.domain.ProtoObject;


public class Heatmap extends ProtoObject<Product> {

    public Double latitude;
    public Double longitude;
    public Integer actions;

}
