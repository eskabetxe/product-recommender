package pro.boto.recommender.aquisiction.provider.flink.actions;

import org.apache.flink.api.common.functions.ReduceFunction;
import pro.boto.recommender.aquisiction.domain.UserAction;


public class UserActionReducer implements ReduceFunction<UserAction> {

    @Override
    public UserAction reduce(UserAction userAction, UserAction t1) throws Exception {
        userAction.incrementViews(t1.getLastAction(),t1.getViews());
        userAction.incrementShares(t1.getLastAction(),t1.getShares());
        userAction.incrementContacts(t1.getLastAction(),t1.getContacts());
        userAction.setReaction(t1.getLastAction(),t1.getReaction());
        return userAction;
    }
}
