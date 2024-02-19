package org.example.util;

import org.apache.flink.table.api.dataview.ListView;

import java.util.ArrayList;
import java.util.List;

public class StddevAccumulator {
    public ListView<Double> nums;

    public StddevAccumulator() {
        nums = new ListView<>();
    }
}
