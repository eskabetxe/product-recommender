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
import pro.boto.protolang.json.ProtoClient;
import pro.boto.recommender.aquisiction.domain.UserEvent;
import pro.boto.recommender.data.tables.event.EventTable;

import java.util.Map;


public class EventSaveBolt extends BaseRichBolt {

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    private OutputCollector collector;
    private KuduTable table;
    private KuduConnector connector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        try {
            this.table = EventTable.kuduTable();
            this.connector = EventTable.kuduConnector();
        }catch(Exception e) {
            LOG.error("event process ko with " + e.getLocalizedMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup() {
        try {
            this.connector.close();
            this.table = null;
            this.collector = null;
        }catch(Exception e) {
            LOG.error("event process ko with " + e.getLocalizedMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String oriJson = tuple.getString(0);
            UserEvent event = ProtoClient.obtainObject(UserEvent.class, oriJson);

            connector.upsert(table, event.toKuduRow());

            collector.emit(tuple, new Values(event));
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
        declarer.declare(new Fields("eventSaved"));
    }
}
