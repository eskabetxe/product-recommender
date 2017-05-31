package pro.boto.recommender.aquisiction.domain;

import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.protolang.domain.ProtoObject;
import pro.boto.protolang.utils.Parser;
import pro.boto.recommender.data.tables.event.EventColumn;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class UserEvent extends ProtoObject<UserEvent> {

    public enum ActionEvent {
        PROPERTY_VIEWED,
        PROPERTY_CONTACTED,
        PROPERTY_SHARED,
        PROPERTY_SAVED,
        PROPERTY_DISCARTED,
        PROPERTY_ROLLBACK
    }

    private String sessionId;
    private String userId;
    private Long propertyId;
    private ActionEvent eventAction;
    private Date eventTime;

    public UserEvent(){}
    public UserEvent(KuduRow row) {
        sessionId = Parser.toString(row.obtain(EventColumn.sessionId()));
        userId = Parser.toString(row.obtain(EventColumn.userId()));
        propertyId = Parser.toLong(row.obtain(EventColumn.productId()));
        eventTime = Parser.toDate(row.obtain(EventColumn.eventTime()));
        eventAction = Parser.toEnum(row.obtain(EventColumn.action()), ActionEvent.class);
    }

    public KuduRow toKuduRow() throws Exception {
        Map<String,Object> value = new HashMap<>();
        value.put(EventColumn.sessionId(), sessionId);
        value.put(EventColumn.userId(), userId);
        value.put(EventColumn.productId(), propertyId);
        value.put(EventColumn.eventTime(), eventTime);
        value.put(EventColumn.action(), eventAction.name());
        return new KuduRow(value);
    }

    public String getSessionId() {
        return sessionId;
    }
    public String getUserId() {
        return userId;
    }
    public Long getProductId() {
        return propertyId;
    }
    public Long getPropertyId() {
        return propertyId;
    }
    public ActionEvent getEventAction() {
        return eventAction;
    }
    public Date getEventTime() {
        return eventTime;
    }

    public boolean isPropertyView(){
        return ActionEvent.PROPERTY_VIEWED.equals(getEventAction());
    }
    public boolean isPropertyContact(){
        return ActionEvent.PROPERTY_CONTACTED.equals(getEventAction());
    }
    public boolean isPropertyShared(){
        return ActionEvent.PROPERTY_SHARED.equals(getEventAction());
    }
    public boolean isPropertySave(){
        return ActionEvent.PROPERTY_SAVED.equals(getEventAction());
    }
    public boolean isPropertyDiscard(){
        return ActionEvent.PROPERTY_DISCARTED.equals(getEventAction());
    }
    public boolean isPropertyRollback(){
        return ActionEvent.PROPERTY_ROLLBACK.equals(getEventAction());
    }


    public boolean hasValidKey(){
        return userId!=null && propertyId!=null;
    }

    public String obtainKeyValue(){
        return userId+"_"+propertyId;
    }



}
