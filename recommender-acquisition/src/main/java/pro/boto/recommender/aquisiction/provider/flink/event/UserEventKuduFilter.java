package pro.boto.recommender.aquisiction.provider.flink.event;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pro.boto.flink.connector.kudu.schema.KuduFilter;
import pro.boto.recommender.aquisiction.domain.UserEvent;
import pro.boto.recommender.data.tables.event.EventColumn;

import java.util.ArrayList;
import java.util.List;

public class UserEventKuduFilter implements WindowFunction<UserEvent, List<KuduFilter>, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<UserEvent> filters, Collector<List<KuduFilter>> collector) throws Exception {
        UserEvent event = filters.iterator().next();
        List<KuduFilter> kuduFilters = new ArrayList<>();
        kuduFilters.add(KuduFilter.Builder.create(EventColumn.userId()).equalTo(event.getUserId()).build());
        kuduFilters.add(KuduFilter.Builder.create(EventColumn.productId()).equalTo(event.getProductId()).build());
        collector.collect(kuduFilters);
    }
}
