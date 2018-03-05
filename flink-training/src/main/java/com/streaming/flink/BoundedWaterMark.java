package com.streaming.flink;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.HashMap;

/**
 * Created by Jying on 2018/1/29.
 */
public class BoundedWaterMark extends BoundedOutOfOrdernessTimestampExtractor<HashMap<String, String>> {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    public BoundedWaterMark(long maxOutOfOrder){
        super(Time.seconds(maxOutOfOrder));
    }

    @Override
    public long extractTimestamp(HashMap<String, String> data) {
        SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
        String d = format.format(Long.valueOf(data.get("timestamp")));
        LOG.info("timestamp:" + data.get("timestamp") + " date:" + d);
        LOG.info("last watermark:" + super.getCurrentWatermark().getTimestamp() + " date: " + format.format(super.getCurrentWatermark().getTimestamp()));

        return Long.valueOf(data.get("timestamp"));
    }
}

