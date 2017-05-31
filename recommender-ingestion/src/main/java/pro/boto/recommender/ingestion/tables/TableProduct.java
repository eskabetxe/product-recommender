package pro.boto.recommender.ingestion.tables;

import org.apache.commons.io.FileUtils;
import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.flink.connector.kudu.schema.KuduConnector;
import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.flink.connector.kudu.schema.KuduTable;
import pro.boto.protolang.utils.Parser;
import pro.boto.recommender.data.tables.product.ProductColumn;
import pro.boto.recommender.data.tables.product.ProductTable;
import pro.boto.recommender.ingestion.IngestConfig;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class TableProduct {


    public static void main(String[] args) throws Exception {
        TableProduct.charge();
    }

    public static void charge() throws Exception {
        KuduTable product = ProductTable.kuduTable();
        KuduConnector connector = ProductTable.kuduConnector();
        connector.deleteTable(product);
        connector.createTable(product);
        chargeData(connector, product);
    }

    private static void chargeData(KuduConnector connector, KuduTable table){
        IngestConfig.SotConfig sot = IngestConfig.obtainSotConfig();
        try {
            String file = "/home/boto/Dropbox/kschool/SoT/SoT_Products.csv";
            final AtomicBoolean firstLine = new AtomicBoolean(true);
            FileUtils.readLines(new File(file), sot.charset())
                    .forEach( line -> {
                        if(firstLine.get()) {
                            firstLine.set(false);
                        }else {
                            insertTable(connector, table, line);
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static boolean insertTable(KuduConnector connector, KuduTable table, String line){
        try{
            KuduRow product = mapProduct(line);
            connector.upsert(table, product);
            return true;
        }catch (Exception e){
            return false;
        }
    }

    private static KuduRow mapProduct(String line) throws KuduException{
        String[] fields = line.split("\\|");
        Map<String,Object> values = new HashMap<>();

        values.put(ProductColumn.productId(), Parser.toLong(fields[0]));
        values.put(ProductColumn.bedrooms(), Parser.toInteger(fields[1]));
        values.put(ProductColumn.bathrooms(), Parser.toInteger(fields[2]));
        values.put(ProductColumn.contructedArea(), Parser.toInteger(fields[3]));
        values.put(ProductColumn.plotArea(), Parser.toInteger(fields[4]));
        values.put(ProductColumn.elevator(), Parser.toBoolean(fields[5]));
        values.put(ProductColumn.parking(), Parser.toBoolean(fields[6]));
        values.put(ProductColumn.condiction(), Parser.toString(fields[7]));
        values.put(ProductColumn.typology(), Parser.toString(fields[8]));
        values.put(ProductColumn.operation(), Parser.toString(fields[9]));
        values.put(ProductColumn.districto(), Parser.toString(fields[10]));
        values.put(ProductColumn.concelho(), Parser.toString(fields[11]));
        values.put(ProductColumn.freguesia(), Parser.toString(fields[12]));
        values.put(ProductColumn.latitude(), Parser.toDouble(fields[13]));
        values.put(ProductColumn.longitude(), Parser.toDouble(fields[14]));

        return new KuduRow(values);
    }

}
