package org.example.model;

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
}
