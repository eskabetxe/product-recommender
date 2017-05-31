package pro.boto.recommender.aquisiction.provider.flink.event;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import pro.boto.recommender.aquisiction.domain.UserEvent;

import javax.annotation.Nullable;

public class UserEventWatermark implements AssignerWithPunctuatedWatermarks<UserEvent> {

    private final static long MAX_TIME_LAG = 1000;

    @Override
    public long extractTimestamp(UserEvent event, long l) {
        return event.getEventTime().getTime();
    }

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(UserEvent userEvent, long l) {
        return new Watermark(l-MAX_TIME_LAG);
    }
}
