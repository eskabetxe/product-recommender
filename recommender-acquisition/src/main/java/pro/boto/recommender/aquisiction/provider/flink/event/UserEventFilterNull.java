package pro.boto.recommender.aquisiction.provider.flink.event;

import org.apache.flink.api.common.functions.FilterFunction;
import pro.boto.recommender.aquisiction.domain.UserEvent;

public class UserEventFilterNull implements FilterFunction<UserEvent> {

    @Override
    public boolean filter(UserEvent event) throws Exception {
        boolean bol = event.getUserId()!=null && event.getProductId()!=null;
        return bol;
    }
}
