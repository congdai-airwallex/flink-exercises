package org.example.util;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.example.model.PreTxData;

public class PreTxDataParserRichFlatMap extends RichFlatMapFunction<String,PreTxData> {
    private Gson gson;
    @Override
    public void open(Configuration parameters) throws Exception {
        gson = new Gson();
    }

    @Override
    public void flatMap(String s, Collector<PreTxData> collector) throws Exception {
        PreTxData data = null;
        try {
            data = gson.fromJson(s, PreTxData.class);
        } catch (Exception e) {

        }
        if(data != null) {
            collector.collect(data);
        }
    }
}
