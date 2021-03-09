package com.asuraflink.sql.user.delay.model;


import com.asuraflink.sql.user.delay.TimeExchange;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 订单表
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Order implements Serializable {
    public Long user;
    public String product;
    public int amount;
    public Timestamp logTime;
    public long orderId;


    public Order(String product, long orderId) {
        this.product = product;
        this.orderId = orderId;
    }

    @Override
    public String toString() {
        return "Order{" +
                "user=" + user +
                ", product='" + product + '\'' +
                ", amount=" + amount +
                ", logTime=" + logTime +
                ", orderId=" + orderId +
                '}';
    }
}
