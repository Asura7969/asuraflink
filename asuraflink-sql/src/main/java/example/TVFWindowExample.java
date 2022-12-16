package example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TVFWindowExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env);

        tbEnv.executeSql(
                "CREATE TABLE t_order (                                     " +
                        "  order_id STRING,                                          " +
                        "  name STRING,                                              " +
                        "  user_id STRING,                                           " +
                        "  create_time TIMESTAMP(0),                                 " +
                        "  price DECIMAL(10, 2),                                     " +
                        "  WATERMARK FOR create_time AS create_time - INTERVAL '10' SECOND    " +
                        ") WITH (                                                    " +
                        "  'connector' = 'jdbc',                                     " +
                        "  'url' = 'jdbc:mysql://localhost:3306/binlog',             " +
                        "  'username' = 'root',                                      " +
                        "  'password' = '123456',                                    " +
                        "  'table-name' = 't_order'                                  " +
                        ")");

        tbEnv.sqlQuery(
                "SELECT * FROM TABLE(                                                " +
                        "  TUMBLE(\n" +
                        "     DATA => TABLE t_order,\n" +
                        "     TIMECOL => DESCRIPTOR(create_time),\n" +
                        "     SIZE => INTERVAL '10' MINUTES))"
        ).execute().print();

//        tbEnv.sqlQuery(
//                "SELECT window_start, window_end, SUM(price)\n" +
//                        "  FROM TABLE(\n" +
//                        "    TUMBLE(TABLE t_order, DESCRIPTOR(create_time), INTERVAL '10' MINUTES))\n" +
//                        "  GROUP BY window_start, window_end"
//        ).execute().print();

    }
}
