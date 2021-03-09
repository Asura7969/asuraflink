package com.asuraflink.sql.user.delay.model;

import com.asuraflink.sql.user.delay.TimeExchange;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * 支付表
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Pay implements Serializable {

    public Long user;
    public int amount;
    public long orderId;
    public long payId;
    public Timestamp ttl;

    public Pay(long payId) {
        this.payId = payId;
    }

//    @Override
//    public long getDelay(TimeUnit unit) {
//        // 计算该任务距离过期还剩多少时间
//        long remaining = ttl - System.currentTimeMillis();
//        return unit.convert(remaining, TimeUnit.MILLISECONDS);
//    }
//
//    @Override
//    public int compareTo(Delayed o) {
//        // 比较、排序：对任务的延时大小进行排序，将延时时间最小的任务放到队列头部
//        return (int) (this.getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS));
//    }

    @Override
    public String toString() {
        return "Pay{" +
                "user=" + user +
                ", amount=" + amount +
                ", orderId=" + orderId +
                ", payId=" + payId +
                ", ttl=" + ttl +
                '}';
    }
}
