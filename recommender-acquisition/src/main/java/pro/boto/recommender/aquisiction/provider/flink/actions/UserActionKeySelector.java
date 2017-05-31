package pro.boto.recommender.aquisiction.provider.flink.actions;

import org.apache.flink.api.java.functions.KeySelector;
import pro.boto.recommender.aquisiction.domain.UserAction;
import pro.boto.recommender.aquisiction.domain.UserEvent;

public class UserActionKeySelector implements KeySelector<UserAction,String>{
    @Override
    public String getKey(UserAction action) throws Exception {
        return action.getUserId()+"_"+action.getUserId();
    }
}
