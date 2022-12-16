package example;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;
import java.util.Random;

import static org.apache.flink.table.api.Expressions.$;

public class LookupJoinExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> mockDs = env.addSource(new MockSource());

//        Table tb = tbEnv.fromDataStream(
//                mockDs,
//                Schema.newBuilder()
//                        .column("x", DataTypes.STRING().notNull())
//                        .columnByExpression("y", $("y").proctime())
//                        .build());

        Table tb = tbEnv.fromDataStream(mockDs, $("x"), $("y").proctime());
        tbEnv.createTemporaryView("v", tb);

        tbEnv.executeSql(
                "CREATE TEMPORARY TABLE t (                " +
                "  order_id STRING,                                 " +
                "  name STRING,                                     " +
                "  user_id STRING,                                  " +
                "  create_time TIMESTAMP(0),                        " +
                "  PRIMARY KEY(order_id) NOT ENFORCED               " +
                ") WITH (                                           " +
                "  'lookup.cache.max-rows' = '2',                   " +
                "  'lookup.cache.ttl' = '1min',                     " +
                "  'connector' = 'jdbc',                            " +
                "  'url' = 'jdbc:mysql://localhost:3306/binlog',    " +
                "  'username' = 'root',                             " +
                "  'password' = '123456',                           " +
                "  'table-name' = 't_order'                         " +
                ")");

        tbEnv.sqlQuery(
                "SELECT * FROM v                                    " +
                "JOIN t                                             " +
                "  FOR SYSTEM_TIME AS OF v.y                        " +
                "  ON v.x=t.order_id"
        ).execute().print();

    }

    public static class MockSource implements SourceFunction<String> {
        private static final List<String> data = Lists.newArrayList("1", "2", "3", "4", "5");
        private final Random random;
        public MockSource() {
            this.random = new Random();
        }

        @Override
        public void run(SourceFunction.SourceContext<String> sc) {
            try {
                for(;;) {
                    Thread.sleep(2000);
                    int index = random.nextInt(6);
                    sc.collect(data.get(index));
                }
            } catch (Exception e) {
                // ignore
            }

        }

        @Override
        public void cancel() {}
    }
}
