package pro.boto.recommender.aquisiction.provider.flink.actions;

import org.apache.flink.api.common.functions.MapFunction;
import pro.boto.recommender.aquisiction.domain.UserAction;
import pro.boto.recommender.aquisiction.domain.UserEvent;


public class UserActionMapper implements MapFunction<UserEvent, UserAction> {

    @Override
    public UserAction map(UserEvent event) throws Exception {
        return calculateAction(event);
    }

    private UserAction calculateAction(UserEvent event) {

        UserAction action = new UserAction(event.getUserId(),event.getProductId());

        if(event.isPropertyContact()) action.incrementContacts(event.getEventTime());
        if(event.isPropertyShared()) action.incrementShares(event.getEventTime());
        if(event.isPropertyView()) action.incrementViews(event.getEventTime());
        if(event.isPropertySave()) action.setPositiveReaction(event.getEventTime());
        if(event.isPropertyRollback()) action.setNeutralReaction(event.getEventTime());
        if(event.isPropertyDiscard()) action.setNegativeReaction(event.getEventTime());

        return action;

    }

}
