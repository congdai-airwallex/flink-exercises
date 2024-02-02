package org.example.util;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.model.Event;

public class EventProcess extends ProcessFunction<String, Event> {
    private final OutputTag<String> errorOutput;
    public EventProcess(OutputTag<String> errorOutput) {
        this.errorOutput = errorOutput;
    }

    @Override
    public void processElement(String s, ProcessFunction<String, Event>.Context context, Collector<Event> collector) throws Exception {
        Event event = Event.parseFromString(s);
        if(event != null) {
            collector.collect(event);
        } else {
            context.output(errorOutput, s);
        }
    }
}
