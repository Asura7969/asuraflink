package com.asuraflink.sql.dynamic.redis;

import io.vertx.core.*;
import io.vertx.redis.client.*;
import io.vertx.redis.client.impl.RedisAPIImpl;
import io.vertx.redis.client.impl.RedisClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

import static com.asuraflink.sql.dynamic.redis.RedisDynamicTableFactory.*;

/**
 * https://cloud.tencent.com/developer/article/1560553
 */
@Slf4j
public class RedisRowDataLookupFunction extends TableFunction<RowData> {
    private static final long serialVersionUID = 1L;

    private final ReadableConfig options;
    private final String command;
    private final String additionalKey;
//    private final int cacheMaxRows;
//    private final int cacheTtlSec;

    private RedisCommandsContainer commandsContainer;
    private RedisClient redisClient;
    private Vertx vertx;

    public RedisRowDataLookupFunction(ReadableConfig options) {
        Preconditions.checkNotNull(options, "No options supplied");
        this.options = options;
        this.additionalKey = options.get(LOOKUP_ADDITIONAL_KEY);

        command = options.get(COMMAND).toUpperCase();
        Preconditions.checkArgument(command.equals("GET") ||
                command.equals("HGET"), "Redis table source only supports GET and HGET commands");

    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        RedisOptions config = new RedisOptions();
        // redis://[username:password@][host][:port][/[database]
        String connectionString = String.format("redis://%s:%s@%s:%d/%d",
                "",options.get(PASSWORD), options.get(SINGLE_HOST),
                options.get(SINGLE_PORT),options.get(DB_NUM));
        config.setConnectionString(connectionString);
        config.setMaxPoolSize(options.get(CONNECTION_MAX_TOTAL));
        config.setMaxPoolWaiting(options.get(CONNECTION_MAX_IDLE));
//        config.setMaxWaitingHandlers()

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(10);
        vo.setWorkerPoolSize(20);

        vertx = Vertx.vertx(vo);
        redisClient = new RedisClient(vertx, config);
        ping();

    }

    private void ping() {
        Future<Response> pingFuture = redisClient.send(Request.cmd(Command.PING))
                .onComplete(res -> {
                    if (res.succeeded()) {
                        log.info("Redis connected successfully!");
                    } else {
                        throw new RuntimeException(res.cause().getMessage());
                    }
                });
        while (!pingFuture.isComplete()){}
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (redisClient != null) {
            redisClient.close();
        }
        if (vertx != null) {
            vertx.close();
        }
    }

    public void eval(Object obj) {
        RowData lookupKey = GenericRowData.of(obj);

        StringData key = lookupKey.getString(0);
        Future<Response> response;
        if (command.equals("GET")) {
            response = redisClient.send(Request.cmd(Command.GET).arg(key.toString()));
        } else {
            response = redisClient.send(Request.cmd(Command.HGET)
                            .arg(additionalKey).arg(key.toString()));
        }
        response.onSuccess(event -> {
            String value = event.toString();
            RowData result = GenericRowData.of(key, StringData.fromString(value));
            collect(result);
        });
    }
}
