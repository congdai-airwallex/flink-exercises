package org.example.model;

import com.google.gson.Gson;

public class PayData {
    public String paymentAttemptId;
    public String partitionKey;
    public Double aggregationKeyMappingValue;
    public Long createAt;
    public PayData() {}
    public PayData(String paymentAttemptId, String partitionKey, Double aggregationKeyMappingValue, Long createAt) {
        this.paymentAttemptId = paymentAttemptId;
        this.partitionKey = partitionKey;
        this.aggregationKeyMappingValue = aggregationKeyMappingValue;
        this.createAt = createAt;
    }
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
