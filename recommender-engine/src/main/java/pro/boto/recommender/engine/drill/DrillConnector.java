package pro.boto.recommender.engine.drill;


import pro.boto.recommender.configuration.DrillConfig;
import pro.boto.recommender.configuration.RecommenderConfig;

import java.sql.*;

public abstract class DrillConnector {

    protected DrillConnector(){}

    protected ResultSet executeQuery(String query) throws SQLException{
        Connection connection = obtainConnection();
        Statement st = connection.createStatement();
        return st.executeQuery(query);
    }

    private Connection obtainConnection() throws SQLException {
        try {
            DrillConfig config = RecommenderConfig.obtainDrillConfig();
            String conn = String.format("jdbc:drill:schema=%s;drillbit=%s;",config.schema(),config.master());
            Class.forName("org.apache.drill.jdbc.Driver");
        return DriverManager.getConnection(conn);
        } catch (ClassNotFoundException e) {
            throw new SQLException("not found drill driver class. "+ e.getLocalizedMessage());
        }
    }

}
