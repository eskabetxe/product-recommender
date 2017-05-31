package pro.boto.recommender.aquisiction.provider.flink;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import pro.boto.flink.connector.kudu.KuduException;
import pro.boto.flink.connector.kudu.async.KuduAsyncStream;
import pro.boto.flink.connector.kudu.function.KuduReadFunction;
import pro.boto.flink.connector.kudu.function.KuduUpsertFunction;
import pro.boto.flink.connector.kudu.schema.KuduFilter;
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
import pro.boto.recommender.data.tables.action.ActionTable;
import pro.boto.recommender.data.tables.event.EventTable;

import java.util.List;

public class AcquisitionMain {

    public static void main(String[] args) throws Exception {
        AcquisitionMain process = new AcquisitionMain();
        process.start();
    }

    public void start() throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);
       // env.setParallelism(20);

        DataStreamSource<String> source = env.addSource(KafkaConsumer.obtain());

        DataStream<UserEvent> savedEvents = saveEvents(source);

        DataStream<UserEvent> relevantEvents = obtainRelevantEvents(savedEvents, Time.seconds(30));

        DataStream<UserAction> uniqueActions = calculateUniqueActionAndSave(relevantEvents);

        env.execute("AcquisitionJob");
    }

    private DataStream<UserEvent> saveEvents(DataStreamSource<String> source) throws KuduException {
        return source
                .map(json -> ProtoClient.obtainObject(UserEvent.class, json))
                .assignTimestampsAndWatermarks(new UserEventWatermark())
                .map(event -> event.toKuduRow())
                .map(new KuduUpsertFunction(EventTable.kuduTable()))
                .map(row -> new UserEvent(row));

    }

    private DataStream<UserEvent> obtainRelevantEvents(DataStream<UserEvent> events, Time windowTime) throws KuduException {
        return events
                .filter(new UserEventFilterNull())
                .keyBy(new UserEventKeySelector())
                .window(EventTimeSessionWindows.withGap(windowTime))
                .apply(new UserEventKuduFilter())
                .flatMap(new KuduReadFunction(EventTable.kuduTable()))
                .map(row -> new UserEvent(row));

    }

    private DataStream<UserAction> calculateUniqueActionAndSave(DataStream<UserEvent> events) throws KuduException {
        return events
                .map(new UserActionMapper())
                .keyBy(new UserActionKeySelector())
                .reduce(new UserActionReducer())
                .map(action -> action.toKuduRow())
                .map(new KuduUpsertFunction(ActionTable.kuduTable()))
                .map(action -> new UserAction(action));

    }
}
