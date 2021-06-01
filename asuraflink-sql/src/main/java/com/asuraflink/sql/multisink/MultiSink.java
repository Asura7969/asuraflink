package com.asuraflink.sql.multisink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author asura7969
 * @create 2021-05-31-22:07
 */
public class MultiSink {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        Configuration configuration = new Configuration();
        configuration.setBoolean("table.optimizer.reuse-optimize-block-with-digest-enabled", true);
        tEnv.getConfig().addConfiguration(configuration);

        DataStreamSource<Tuple2<String, String>> continueSource = env.addSource(new ContinueSource());
        Table t = tEnv.fromDataStream(continueSource,
                $("key"),
                $("data"),
                $("proctime").proctime());

        tEnv.createTemporaryView("source_stream", t);

        tEnv.executeSql(
                "CREATE TABLE good_sink (data varchar) WITH (" +
                   "'connector' = 'print'" +
                   ")"
        );

        tEnv.executeSql(
                "CREATE TABLE atomic_sink (data varchar) WITH (" +
                        "'connector' = 'print'" +
                        ")"
        );

        tEnv.executeSql(
                "CREATE TABLE bad_sink (data varchar) WITH (" +
                        "'connector' = 'print'" +
                        ")"
        );

        tEnv.executeSql(
                "insert into good_sink select data from source_stream where `key` = 'good'"
        );

        tEnv.executeSql(
                "insert into atomic_sink select data from source_stream where `key` = 'atomic'"
        );

        tEnv.executeSql(
                "insert into bad_sink select data from source_stream where `key` = 'bad'"
        );


    }

    public static class ContinueSource implements SourceFunction<Tuple2<String, String>> {
        private volatile boolean isRunning = true;
        private int count = 0;
        @Override
        public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
            while (isRunning && count < Integer.MAX_VALUE) {
                synchronized (ctx.getCheckpointLock()) {
                    final long time = System.currentTimeMillis() / 1000;
                    if (time % 3 == 0) {
                        ctx.collect(new Tuple2<>("good", "" + count));
                    } else if (time % 2 == 0) {
                        ctx.collect(new Tuple2<>("atomic", "" + count));
                    } else {
                        ctx.collect(new Tuple2<>("bad", "" + count));
                    }

                    count++;
                    Thread.sleep(1000);
                }
            }

        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
