package pro.boto.recommender.aquisiction.provider.flink;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.flink.connector.kudu.async.KuduAsyncStream;
import pro.boto.flink.connector.kudu.schema.KuduFilter;
import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.protolang.json.ProtoClient;
import pro.boto.recommender.aquisiction.domain.UserAction;
import pro.boto.recommender.aquisiction.domain.UserEvent;
import pro.boto.recommender.aquisiction.provider.flink.actions.UserActionKeySelector;
import pro.boto.recommender.aquisiction.provider.flink.actions.UserActionMapper;
import pro.boto.recommender.aquisiction.provider.flink.actions.UserActionReducer;
import pro.boto.recommender.aquisiction.provider.flink.event.UserEventFilterNull;
import pro.boto.recommender.aquisiction.provider.flink.event.UserEventKeySelector;
import pro.boto.recommender.aquisiction.provider.flink.event.UserEventKuduFilter;
import pro.boto.recommender.aquisiction.provider.flink.event.UserEventWatermark;
import pro.boto.recommender.configuration.KafkaConfig;
import pro.boto.recommender.data.tables.action.ActionTable;
import pro.boto.recommender.data.tables.event.EventTable;

import java.util.List;

public class AcquisitionAsyncMain {

    public static void main(String[] args) throws Exception {
        AcquisitionAsyncMain process = new AcquisitionAsyncMain();
        process.start();
    }

    public void start() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);

        DataStreamSource<String> source = env.addSource(KafkaConsumer.obtain());

        DataStream<UserEvent> savedEvents = saveEvents(source);

        DataStream<UserEvent> relevantEvents = obtainRelevantEvents(savedEvents, Time.seconds(30));

        DataStream<UserAction> uniqueActions = calculateUniqueActionAndSave(relevantEvents);

        env.execute("AcquisitionAsyncJob");
    }

    private DataStream<UserEvent> saveEvents(DataStreamSource<String> source) throws KuduException {
        DataStream<KuduRow> events = source
                .map(json -> ProtoClient.obtainObject(UserEvent.class, json))
                .assignTimestampsAndWatermarks(new UserEventWatermark())
                .map(e -> e.toKuduRow());

        return KuduAsyncStream
                .unorderedUpsert(events, EventTable.kuduTable())
                .map(row -> new UserEvent(row));
    }

    private DataStream<UserEvent> obtainRelevantEvents(DataStream<UserEvent> events, Time windowTime) throws KuduException {
        DataStream<List<KuduFilter>> eventFilter = events
                .filter(new UserEventFilterNull())
                .keyBy(new UserEventKeySelector())
                .window(EventTimeSessionWindows.withGap(windowTime))
                .apply(new UserEventKuduFilter());

        return KuduAsyncStream.unorderedReader(eventFilter, EventTable.kuduTable())
                .map(row -> new UserEvent(row));

    }

    private DataStream<UserAction> calculateUniqueActionAndSave(DataStream<UserEvent> events) throws KuduException {
        DataStream<UserAction> uniqueAction = events
                .map(new UserActionMapper())
                .keyBy(new UserActionKeySelector())
                .reduce(new UserActionReducer());

        return KuduAsyncStream
                .unorderedUpsert(uniqueAction.map(a -> a.toKuduRow()), ActionTable.kuduTable())
                .map(action -> new UserAction(action));

    }
}
