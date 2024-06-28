package org.example.util;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.example.model.PrePostTxData;

import static org.example.util.FlinkUtil.logInfo;

public class PrePostTxDataParserRichFlatMap extends RichFlatMapFunction<String, PrePostTxData> {
    private Gson gson;
    @Override
    public void open(Configuration parameters) throws Exception {
        gson = new Gson();
    }

    @Override
    public void flatMap(String s, Collector<PrePostTxData> collector) throws Exception {
        PrePostTxData data = null;
        try {
            logInfo("flatMap: %s\n", s);
            data = gson.fromJson(s, PrePostTxData.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(data != null) {
            collector.collect(data);
        }
    }
}
