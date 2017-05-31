package pro.boto.recommender.engine.drill;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import pro.boto.recommender.engine.manager.domain.Heatmap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Service
public class HeatmapConnector extends DrillConnector {

    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final static String QUERY_EVENTS =
            " select p.latitude, p.longitude, count(1) actions " +
                    " from kudu.userevents r " +
                    " inner join kudu.products p on p.productid=r.productid " +
                    " where r.eventTime >= '%s' " +
                    " and r.eventTime < '%s' " +
                    " and r.action = 'PROPERTY_%s' " +
                    " group by p.latitude, p.longitude ";

    public List<Heatmap> obtainActionsSaved(String start, String end)  {
        try {
            ResultSet rs = executeQuery(String.format(QUERY_EVENTS, start, end, "SAVED"));
            return mapHeatmap(rs);
        } catch (SQLException e) {
            LOG.error("error obtaining saved. "+ e.getLocalizedMessage(), e);
            return new ArrayList<>();
        }
    }

    public List<Heatmap> obtainActionsContacts(String start, String end)  {
        try {
            ResultSet rs = executeQuery(String.format(QUERY_EVENTS, start, end, "CONTACTED"));
            return mapHeatmap(rs);
        } catch (SQLException e) {
            LOG.error("error obtaining contacts. "+ e.getLocalizedMessage(), e);
            return new ArrayList<>();
        }
    }

    public List<Heatmap> obtainActionsShares(String start, String end)  {
        try {
            ResultSet rs = executeQuery(String.format(QUERY_EVENTS, start, end,"SHARED"));
            return mapHeatmap(rs);
        } catch (SQLException e) {
            LOG.error("error obtaining shares. "+ e.getLocalizedMessage(), e);
            return new ArrayList<>();
        }
    }

    public List<Heatmap> obtainActionsViews(String start, String end)  {
        try {
            ResultSet rs = executeQuery(String.format(QUERY_EVENTS, start, end, "VIEWED"));
            return mapHeatmap(rs);
        } catch (SQLException e) {
            LOG.error("error obtaining views. "+ e.getLocalizedMessage(), e);
            return new ArrayList<>();
        }
    }

    private List<Heatmap> mapHeatmap(ResultSet rs) throws SQLException{
        List<Heatmap> values = new ArrayList<>();
        while(rs.next()){
            Heatmap heat = new Heatmap();
            heat.latitude = rs.getDouble("latitude");
            heat.longitude = rs.getDouble("longitude");
            heat.actions = rs.getInt("actions");
            values.add(heat);
        }
        return values;
    }
}
