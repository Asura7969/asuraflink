package com.asuraflink.window;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Comparator;

/**
 * @author asura7969
 * @create 2021-03-19-8:46
 */
public class SortTimeWindow implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
        return 0;
    }
}
