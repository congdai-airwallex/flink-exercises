package org.example.util;

import org.apache.flink.api.common.eventtime.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.example.util.FlinkUtil.logInfo;

public class BoundedOutOfOrdernessStrategy<T> implements WatermarkStrategy {
    private static final Logger logger = LoggerFactory.getLogger(BoundedOutOfOrdernessStrategy.class);

    class BoundedOutOfOrdernessGenerator<T> implements WatermarkGenerator<T> {
        private long currentMaxTimestamp;
        private final long maxOutOfOrderness;
        private final String name;
        public BoundedOutOfOrdernessGenerator(String name, long maxOutOfOrderness) {
            this.name = name;
            this.maxOutOfOrderness = maxOutOfOrderness;
        }

        /* onEvent is not on time as I expected, sometimes after this event has been sinked, the onEvent is triggered*/
        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            logInfo("%s, onEvent: %s, eventTimestamp: %d\n", name, event, eventTimestamp);
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
            //output.emitWatermark(new Watermark(currentMaxTimestamp-maxOutOfOrderness));
        }

        /* onPeriodicEmit like a crontab job, emit watermark every 200 ms, set by env.getConfig.setAutoWatermarkInterval*/
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // emit the watermark as current highest timestamp minus the out-of-orderness bound
            logInfo("%s, onPeriodicEmit: emitWatermark: %d\n", name, currentMaxTimestamp-maxOutOfOrderness);
            output.emitWatermark(new Watermark(currentMaxTimestamp-maxOutOfOrderness));
        }
    }
    private final long maxOutOfOrderness;
    private final String name;
    public BoundedOutOfOrdernessStrategy(String name, long maxOutOfOrderness) {
        this.name = name;
        this.maxOutOfOrderness = maxOutOfOrderness;
    }
    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new BoundedOutOfOrdernessGenerator(name, maxOutOfOrderness);
    }
}
