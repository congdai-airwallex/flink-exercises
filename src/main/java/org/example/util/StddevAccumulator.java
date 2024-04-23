package org.example.util;

import org.apache.flink.table.api.dataview.ListView;

public class StddevAccumulator {
    public ListView<Double> nums = new ListView<>();
    public int count = 0;
    public double sum = 0.0;
}
