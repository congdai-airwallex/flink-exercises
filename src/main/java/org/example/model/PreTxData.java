package org.example.model;

import com.google.gson.Gson;

public class PreTxData {
    public String paymentIntentId;
    public String paymentAttemptId;
    public String deviceIp;
    public Long createAt;
    public Integer num;
    public PreTxData() {}

    public PreTxData(String paymentIntentId, String paymentAttemptId, String deviceIp, Long createAt, Integer value) {
        this.paymentIntentId = paymentIntentId;
        this.paymentAttemptId = paymentAttemptId;
        this.deviceIp = deviceIp;
        this.createAt = createAt;
        this.num = value;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
