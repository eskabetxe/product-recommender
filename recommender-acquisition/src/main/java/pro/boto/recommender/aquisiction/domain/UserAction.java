package pro.boto.recommender.aquisiction.domain;

import pro.boto.flink.connector.kudu.schema.KuduRow;
import pro.boto.protolang.domain.ProtoObject;
import pro.boto.protolang.utils.Parser;
import pro.boto.recommender.data.tables.action.ActionColumn;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class UserAction extends ProtoObject<UserAction> {

    public enum Reaction {POSITIVE,NEUTRAL,NEGATIVE}


    private String userId;
    private Long productId;
    private Reaction reaction;
    private Integer views;
    private Integer contacts;
    private Integer shares;
    private Date lastAction;


    public UserAction(String userId,Long productId){
        this.userId = userId;
        this.productId = productId;
        reaction = null;
        views = 0;
        contacts = 0;
        shares = 0;
    }
    public UserAction(KuduRow row) {
        userId = Parser.toString(row.obtain(ActionColumn.userId()));
        productId = Parser.toLong(row.obtain(ActionColumn.productId()));
        lastAction = Parser.toDate(row.obtain(ActionColumn.lastAction()));
        reaction = Parser.toEnum(row.obtain(ActionColumn.reaction()),Reaction.class);
        views = Parser.toInteger(row.obtain(ActionColumn.views()));
        contacts = Parser.toInteger(row.obtain(ActionColumn.contacts()));
        shares = Parser.toInteger(row.obtain(ActionColumn.shares()));
    }

    public String getUserId() {
        return userId;
    }
    public Long getProductId() {
        return productId;
    }
    public Reaction getReaction() {
        return reaction;
    }
    public Integer getViews() {
        return views;
    }
    public Integer getContacts() {
        return contacts;
    }
    public Integer getShares() {
        return shares;
    }
    public Date getLastAction() {
        return lastAction;
    }

    public void incrementViews(Date action) {
        this.incrementViews(action,1);
    }
    public void incrementViews(Date action, Integer views) {
        if(views==null || views < 0) return;
        modifyLastAction(action);
        this.views += views;
    }
    public void incrementContacts(Date action) {
        this.incrementContacts(action,1);
    }
    public void incrementContacts(Date action, Integer contacts) {
        if(contacts==null || contacts < 0) return;
        modifyLastAction(action);
        this.contacts = contacts;
    }
    public void incrementShares(Date action) {
        this.incrementShares(action,1);
    }
    public void incrementShares(Date action, Integer shares) {
        if(shares==null || shares < 0) return;
        modifyLastAction(action);
        this.shares = shares;
    }

    public void setPositiveReaction(Date action) {
        setReaction(action,Reaction.POSITIVE);
    }
    public void setNeutralReaction(Date action) {
        setReaction(action,Reaction.NEUTRAL);
    }
    public void setNegativeReaction(Date action) {
        setReaction(action,Reaction.NEGATIVE);
    }

    private Date reactionDate = null;
    public void setReaction(Date action, Reaction reaction) {
        if(reaction==null) return;
        modifyLastAction(action);

        if(this.reactionDate!=null && this.reactionDate.compareTo(action)>0) {
            this.reaction = reaction;
            this.reactionDate = action;
        }
    }

    private void modifyLastAction(Date lastAction) {
        if(lastAction==null) return;
        if(this.lastAction!=null && this.lastAction.compareTo(lastAction)>0) return;
        this.lastAction = lastAction;
    }

    public KuduRow toKuduRow() throws Exception {
        Map<String,Object> value = new HashMap<>();
        value.put(ActionColumn.userId(),getUserId());
        value.put(ActionColumn.productId(), getProductId());
        value.put(ActionColumn.lastAction(),getLastAction());
        value.put(ActionColumn.views(),getViews());
        value.put(ActionColumn.contacts(),getContacts());
        value.put(ActionColumn.shares(),getShares());
        if(getReaction()!=null) {
            value.put(ActionColumn.reaction(), getReaction().name());
        }
        return new KuduRow(value);
    }
}
