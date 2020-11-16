package com.asuraflink.common.async;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

public class HbaseDimensionSource<IN, OUT> extends AsyncFunc<IN, OUT>{
    @Override
    public void init(Configuration parameters) {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void doAsyncInvoke(IN input, ResultFuture<OUT> resultFuture) {

    }
}
