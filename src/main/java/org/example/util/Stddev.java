package org.example.util;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.List;

public class Stddev extends AggregateFunction<Double, StddevAccumulator> {
    @Override
    public Double getValue(StddevAccumulator acc) {
        if(acc.count == 0 || acc.count == 1) {
            return null;
        }
        List<Double> list = acc.nums.getList();
        double avg = acc.sum / acc.count;
        double squaredDiffSum = 0.0;
        for(double num : list) {
            squaredDiffSum  += Math.pow(num - avg, 2);
        }
        return Math.sqrt(squaredDiffSum/(acc.count-1));
    }

    @Override
    public StddevAccumulator createAccumulator() {
        return new StddevAccumulator();
    }

    public void accumulate(StddevAccumulator acc, Double value) throws Exception {
        if(value == null) return;
        acc.count++;
        acc.sum += value;
        acc.nums.add(value);
    }

    public void retract(StddevAccumulator acc, Double value) throws Exception {
        if(value == null) return;
        acc.count--;
        acc.sum -= value;
        acc.nums.remove(value);
    }

    public void resetAccumulator(StddevAccumulator acc) {
        acc.count = 0;
        acc.sum = 0.0;
        acc.nums.clear();
    }
}
