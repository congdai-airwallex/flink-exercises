package org.example.model;

import com.google.gson.Gson;

public class PayFilterData {
    public String clientId;
    public String paymentId;
    public Double amountusd;
    public Integer filtered;
    public Long createAt;

    public PayFilterData() {

    }

    public PayFilterData(String clientId, String paymentId, Double amountusd,
                         Integer filtered, Long createAt) {
        this.clientId = clientId;
        this.paymentId = paymentId;
        this.amountusd = amountusd;
        this.filtered = filtered;
        this.createAt = createAt;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
