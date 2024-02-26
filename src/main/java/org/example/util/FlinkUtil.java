package org.example.util;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class FlinkUtil {
    public static void initEnvironment(StreamExecutionEnvironment env) {
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setStateBackend(new HashMapStateBackend());
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(6000);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointStorage("file:///Users/cong.dai/flink-checkpoint/");

        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.SECONDS),
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
        ));
    }
}
