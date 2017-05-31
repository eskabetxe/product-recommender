package pro.boto.recommender.aquisiction.provider.storm.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.boto.flink.connector.kudu.schema.KuduConnector;
import pro.boto.flink.connector.kudu.schema.KuduFilter;
import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.flink.connector.kudu.schema.KuduTable;
import pro.boto.recommender.aquisiction.domain.UserAction;
import pro.boto.recommender.aquisiction.domain.UserEvent;
import pro.boto.recommender.data.tables.action.ActionColumn;
import pro.boto.recommender.data.tables.event.EventTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class ActionCalculatorBolt extends BaseRichBolt {

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    private OutputCollector collector;
    private KuduTable table;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            UserEvent event = (UserEvent) tuple.getValueByField("eventSaved");

            if(event.getUserId()==null || event.getProductId()==null){
                collector.ack(tuple);
                return;
            }

            List<KuduFilter> filters = new ArrayList<>();
            filters.add(KuduFilter.Builder.create(ActionColumn.userId()).equalTo(event.getUserId()).build());
            filters.add(KuduFilter.Builder.create(ActionColumn.productId()).equalTo(event.getProductId()).build());

            KuduTable table = EventTable.kuduTable();
            KuduConnector connector = EventTable.kuduConnector();

            List<KuduRow> rows = connector.read(table, filters);

            UserAction action = new UserAction(event.getUserId(),event.getPropertyId());
            for (KuduRow row:rows) {
                UserEvent eventRow = new UserEvent(row);
                if(eventRow.isPropertyContact()) action.incrementContacts(eventRow.getEventTime());
                if(eventRow.isPropertyShared()) action.incrementShares(eventRow.getEventTime());
                if(eventRow.isPropertyView()) action.incrementViews(eventRow.getEventTime());
                if(eventRow.isPropertySave()) action.setPositiveReaction(eventRow.getEventTime());
                if(eventRow.isPropertyRollback()) action.setNeutralReaction(eventRow.getEventTime());
                if(eventRow.isPropertyDiscard()) action.setNegativeReaction(eventRow.getEventTime());
            }

            collector.emit(tuple, new Values(action));
            collector.ack(tuple);
            LOG.debug("event process ok");
        }catch(Exception e) {
            collector.reportError(e);
            collector.fail(tuple);
            LOG.error("event process ko with " + e.getLocalizedMessage(), e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userAction"));
    }
}
