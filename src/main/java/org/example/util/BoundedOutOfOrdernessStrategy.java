package org.example.util;

import org.apache.flink.api.common.eventtime.*;

public class BoundedOutOfOrdernessStrategy<T> implements WatermarkStrategy {
    class BoundedOutOfOrdernessGenerator<T> implements WatermarkGenerator<T> {
        private long currentMaxTimestamp;
        private final long maxOutOfOrderness;
        public BoundedOutOfOrdernessGenerator(long maxOutOfOrderness) {
            this.maxOutOfOrderness = maxOutOfOrderness;
        }

        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
            //output.emitWatermark(new Watermark(currentMaxTimestamp-maxOutOfOrderness));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // emit the watermark as current highest timestamp minus the out-of-orderness bound
            output.emitWatermark(new Watermark(currentMaxTimestamp-maxOutOfOrderness));
        }
    }
    private final long maxOutOfOrderness;
    public BoundedOutOfOrdernessStrategy(long maxOutOfOrderness) {
        this.maxOutOfOrderness = maxOutOfOrderness;
    }
    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new BoundedOutOfOrdernessGenerator(maxOutOfOrderness);
    }
}
