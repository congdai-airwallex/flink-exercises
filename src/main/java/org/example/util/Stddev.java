package org.example.util;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.List;

public class Stddev extends AggregateFunction<Double, StddevAccumulator> {

    @Override
    public Double getValue(StddevAccumulator acc) {
        List<Double> list = acc.nums.getList();
        if(list.size() == 0 || list.size() == 1) {
            return null;
        } else {
            Double avg = list.stream().mapToDouble(a -> a).average().orElse(0.0);
            Double sum = 0.0;
            for(Double num : list) {
                sum += (num-avg)*(num-avg);
            }
            return Math.sqrt(sum/(list.size()-1));
        }
    }

    @Override
    public StddevAccumulator createAccumulator() {
        return new StddevAccumulator();
    }

    public void accumulate(StddevAccumulator acc, Double value) throws Exception {
        acc.nums.add(value);
    }

    public void retract(StddevAccumulator acc, Double value) throws Exception {
        acc.nums.remove(value);
    }

    public void resetAccumulator(StddevAccumulator acc) {
        acc.nums.clear();
    }
}
