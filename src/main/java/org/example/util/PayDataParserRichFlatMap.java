package org.example.util;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.example.model.PayData;
import org.example.model.PayFilterData;

public class PayDataParserRichFlatMap extends RichFlatMapFunction<String, PayData> {
    private Gson gson;
    @Override
    public void open(Configuration parameters) throws Exception {
        gson = new Gson();
    }

    @Override
    public void flatMap(String s, Collector<PayData> collector) throws Exception {
        PayData data = null;
        try {
            data = gson.fromJson(s, PayData.class);
        } catch (Exception e) {

        }
        if(data != null) {
            collector.collect(data);
        }
    }
}
