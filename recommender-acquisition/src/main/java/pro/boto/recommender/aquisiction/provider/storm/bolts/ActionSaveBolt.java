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
import pro.boto.flink.connector.kudu.schema.KuduTable;
import pro.boto.recommender.aquisiction.domain.UserAction;
import pro.boto.recommender.data.tables.action.ActionTable;

import java.util.Map;


public class ActionSaveBolt extends BaseRichBolt {

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
            UserAction action = (UserAction) tuple.getValueByField("userAction");

            KuduTable table = ActionTable.kuduTable();
            KuduConnector connector = ActionTable.kuduConnector();

            connector.upsert(table, action.toKuduRow());

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
        declarer.declare(new Fields("actionSave"));
    }
}
