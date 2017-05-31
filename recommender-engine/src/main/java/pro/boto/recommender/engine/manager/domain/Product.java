package pro.boto.recommender.engine.manager.domain;

import pro.boto.protolang.domain.ProtoObject;

public class Product extends ProtoObject<Product> {

    public Long productId;
    public String districto;
    public String concelho;
    public String freguesia;
    public Double latitude;
    public Double longitude;
    public String typology;
    public String operation;
    public String condiction;
    public Integer bedrooms;
    public Integer bathrooms;
    public Integer contructedArea;
    public Integer plotArea;
    public Boolean elevator;
    public Boolean parking;
}
