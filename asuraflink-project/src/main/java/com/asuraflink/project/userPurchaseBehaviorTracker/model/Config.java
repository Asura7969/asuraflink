package com.asuraflink.project.userPurchaseBehaviorTracker.model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

@Data
@ToString
public class Config implements Serializable {
    private static final long serialVersionUID = 4416826133199103653L;

    //{"channel":"APP","registerDate":"2018-01-01","historyPurchaseTimes":0,"maxPurchasePathLength":3}

    private String channel;
    private String registerDate;
    private Integer historyPurchaseTimes;
    private Integer maxPurchasePathLength;

    public Config() {
    }

    public Config(String channel, String registerDate, Integer historyPurchaseTimes, Integer maxPurchasePathLength) {
        this.channel = channel;
        this.registerDate = registerDate;
        this.historyPurchaseTimes = historyPurchaseTimes;
        this.maxPurchasePathLength = maxPurchasePathLength;
    }
}
