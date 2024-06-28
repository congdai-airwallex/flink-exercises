package org.example.model;

import com.google.gson.Gson;

public class AmountDataV2 {
    public String id;
    public Double amount;
    public Long ts;
    public AmountDataV2() {}
    public AmountDataV2(String id, Double amount, Long ts) {
        this.id = id;
        this.amount = amount;
        this.ts = ts;
    }
    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
