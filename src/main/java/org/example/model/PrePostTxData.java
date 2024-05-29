package org.example.model;

import java.io.Serializable;

public class PrePostTxData implements Serializable {
    public String paymentIntentId;
    public String paymentAttemptId;
    public String token;
    public String merchant;
    public String deviceIp;
    public String address;
    public String status;
    public Double amount;
    public Long createAt;
    public PrePostTxData() {}
    public PrePostTxData(String paymentIntentId, String paymentAttemptId, String token,
                       String merchant, String deviceIp, String address, String status, Double amount, Long createAt) {
        this.paymentIntentId = paymentIntentId;
        this.paymentAttemptId = paymentAttemptId;
        this.token = token;
        this.merchant = merchant;
        this.deviceIp = deviceIp;
        this.address = address;
        this.status = status;
        this.amount = amount;
        this.createAt = createAt;
    }
}
