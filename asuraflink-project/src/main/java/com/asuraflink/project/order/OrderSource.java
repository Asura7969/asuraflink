package com.asuraflink.project.order;

import com.asuraflink.common.source.CustomIntervalSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Random;

public class OrderSource extends CustomIntervalSource<Tuple2<String,Double>> {

    String category[] = {
            "女装", "男装",
            "图书", "家电",
            "洗护", "美妆",
            "运动", "游戏",
            "户外", "家具",
            "乐器", "办公"
    };

    @Override
    protected Tuple2<String, Double> getMsg(Random random) {
        String c = category[(int) (Math.random() * (category.length - 1))];
        double price = random.nextDouble() * 100;
        return Tuple2.of(c, price);
    }

    @Override
    protected int getIntervalOfSeconds() {
        return 5;
    }
}
