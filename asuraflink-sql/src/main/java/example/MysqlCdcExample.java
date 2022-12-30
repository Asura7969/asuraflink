package example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class MysqlCdcExample {


    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);


        String sourceTable =
                "CREATE TABLE ods_mysql_t_order (" +
                "       order_id            STRING PRIMARY KEY NOT ENFORCED ," +
                "       name                STRING                          ," +
                "       user_id             STRING                          ," +
                "       create_time         TIMESTAMP(0)                    ," +
                "       proc_time as proctime()                              " +
                ") WITH ( " +
                "       'connector' = 'mysql-cdc',                           " +
                "       'hostname'  = 'localhost',                           " +
                "       'port'      = '3306',                                " +
                "       'username'  = 'root',                                " +
                "       'password'  = '123456',                              " +
                "       'server-time-zone' = 'Asia/Shanghai',                " +
                "       'database-name' = 'binlog',                          " +
                "       'table-name'    = 't_order'                          " +
                ")";

        String sinkTable =
                "CREATE TABLE ads_kafka (                                    " +
                "       user_id STRING,                                      " +
                "       c BIGINT,                                            " +
                "       PRIMARY KEY (user_id) NOT ENFORCED                   " +
                ") WITH (                                                    " +
                "       'connector' = 'upsert-kafka',                        " +
                "       'topic' = 'user_order_count',                        " +
                "       'properties.bootstrap.servers' = 'localhost:9092',   " +
                "       'key.format' = 'csv',                                " +
                "       'value.format' = 'csv'                               " +
                ")";

        String transformSQL =
                "INSERT INTO ads_kafka " +
                "SELECT user_id, COUNT(1) as c " +
                "FROM ods_mysql_t_order " +
                "GROUP BY user_id";

        tableEnv.executeSql(sourceTable);
        tableEnv.executeSql(sinkTable);
        System.out.println(tableEnv.explainSql(transformSQL));
        TableResult result = tableEnv.executeSql(transformSQL);
        result.await();
    }
}
