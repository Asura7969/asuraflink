package com.asuraflink.common.async;

import com.asuraflink.common.async.config.JDBCDimensionSourceConfig;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.util.Objects;

public class JDBCDimensionSource<IN, OUT> extends AsyncFunc<IN, OUT>{

    private transient SQLClient sqlClient;

    public JDBCDimensionSource(JDBCDimensionSourceConfig jdbcDimensionSourceConfig){
        this.dimensionSourceConfig = jdbcDimensionSourceConfig;
    }

    @Override
    public void init(Configuration parameters) {
        if(Objects.isNull(dimensionSourceConfig)){
            throw new IllegalArgumentException("jdbcDimensionSourceConfig is null");
        }
        JDBCDimensionSourceConfig jdbcDimensionSourceConfig = (JDBCDimensionSourceConfig)dimensionSourceConfig;
        Vertx vertx = Vertx.vertx(
                new VertxOptions()
                        .setWorkerPoolSize(10)
                        .setEventLoopPoolSize(5)
        );

        JsonObject config = new JsonObject()
                .put("url", jdbcDimensionSourceConfig.getUrl())
                .put("driver_class", jdbcDimensionSourceConfig.getDriverClass())
                .put("max_pool_size", jdbcDimensionSourceConfig.getMaxPoolSize())
                .put("user", jdbcDimensionSourceConfig.getUserName())
                .put("password", jdbcDimensionSourceConfig.getPassword());

        sqlClient = JDBCClient.createShared(vertx, config);
    }

    @Override
    public void shutdown() {
        sqlClient.close();
    }

    @Override
    public void doAsyncInvoke(IN input, ResultFuture<OUT> resultFuture) {

    }

}
