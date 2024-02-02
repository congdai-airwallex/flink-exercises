package org.example.model;

import com.google.gson.Gson;

public class AmountData {
    public String id;
    public Double amount;
    public AmountData() {}
    public AmountData(String id, Double amount) {
        this.id = id;
        this.amount = amount;
    }
    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
