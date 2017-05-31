package pro.boto.recommender.aquisiction.provider.flink.event;

import org.apache.flink.api.java.functions.KeySelector;
import pro.boto.recommender.aquisiction.domain.UserEvent;

public class UserEventKeySelector implements KeySelector<UserEvent,String>{
    @Override
    public String getKey(UserEvent event) throws Exception {
        return event.getUserId()+"_"+event.getUserId();
    }
}
