package pro.boto.recommender.ingestion.tables;

import pro.boto.flink.connector.kudu.schema.KuduConnector;
import pro.boto.flink.connector.kudu.schema.KuduTable;
import pro.boto.recommender.data.tables.action.ActionTable;
import pro.boto.recommender.data.tables.event.EventTable;
import pro.boto.recommender.data.tables.product.ProductTable;

public class TableCreation {

    public static void main(String[] args) throws Exception {
        TableCreation.start();
    }

    public static void start() throws Exception {

        KuduTable events = EventTable.kuduTable();
        KuduConnector eventsConn = EventTable.kuduConnector();
        eventsConn.deleteTable(events);
        eventsConn.createTable(events);

        KuduTable actions = ActionTable.kuduTable();
        KuduConnector actionsConn = ActionTable.kuduConnector();
        actionsConn.deleteTable(actions);
        actionsConn.createTable(actions);

        KuduTable product = ProductTable.kuduTable();
        KuduConnector productConn = ProductTable.kuduConnector();
        productConn.deleteTable(product);
        productConn.createTable(product);

    }

}
