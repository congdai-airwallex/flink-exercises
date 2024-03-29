package org.example.window;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.model.Event;
import org.example.model.PayData;
import org.example.util.BoundedOutOfOrdernessStrategy;
import org.example.util.EventProcess;
import org.example.util.FlinkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class WindowMain {
    private static final Logger logger = LoggerFactory.getLogger(WindowMain.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkUtil.initEnvironment(env);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("test")
                .setTopics("test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> source2 = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("test")
                .setTopics("test2")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> sourceStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<String> sourceStream2 = env
                .fromSource(source2, WatermarkStrategy.noWatermarks(), "Kafka Source2");

        OutputTag<String> errorOutput = new OutputTag<String>("error") {};

        SingleOutputStreamOperator<Event> eventDataStream = sourceStream.process(new EventProcess(errorOutput));

        SingleOutputStreamOperator<Event> eventDataStream2 = sourceStream2.process(new EventProcess(errorOutput));

        DataStream<Event> eventTimeData = eventDataStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((event, timestamp) -> event.timestamp)
                                .withIdleness(Duration.ofMillis(300))
                );

        DataStream<Event> eventTimeData2 = eventDataStream2
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((event, timestamp) -> event.timestamp)
                                .withIdleness(Duration.ofMillis(300))
                );

        WatermarkStrategy<Event> wt = new BoundedOutOfOrdernessStrategy<>(0L);
        DataStream<Event> reducedStream = eventTimeData.union(eventTimeData2)
                .assignTimestampsAndWatermarks(
                        wt.withTimestampAssigner((event, timestamp) -> event.timestamp).withIdleness(Duration.ofSeconds(1))
                )
                .keyBy(x -> x.type)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .allowedLateness(Time.days(1))
                .process(new ProcessWindowFunction<Event, Event, String, TimeWindow> () {
                    private ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<>("pairState", String.class));
                    }
                    @Override
                    public void process(String key, Context context, Iterable<Event> iterable, Collector<Event> collector) throws Exception {
                        Event result = new Event(0L, "", "");
                        List<Event> l = new ArrayList<>();

                        for(Event event : iterable) {
                            l.add(event);
                        }
                        l.sort((o1, o2) -> (int) (o1.timestamp - o2.timestamp));
                        for(Event event : l) {
                            result.type = event.type;
                            result.message += event.message;
                            state.update(event.message);
                            collector.collect(result);
                        }
                    }
                }).filter(x -> x != null).keyBy(x -> x.type).map(x -> new Event(x.timestamp, ""+x.type, x.message));

        reducedStream.print();

        DataStream<String> errorStream = eventDataStream.getSideOutput(errorOutput);
        errorStream.print();

        //env.execute("WindowFlink");
        System.out.println(env.getExecutionPlan());
    }
}