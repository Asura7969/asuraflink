package example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RunTimeExample {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env);
//        tbEnv.getConfig().set("collect-lineage-impl", "log");
        tbEnv.executeSql(
                "CREATE TABLE source1 (                                     " +
                        "  a STRING,                                                 " +
                        "  b INT,                                                    " +
                        "  c INT                                                     " +
                        ") WITH (                                                    " +
                        "  'connector' = 'jdbc',                                     " +
                        "  'url' = 'jdbc:mysql://localhost:3306/binlog',             " +
                        "  'username' = 'root',                                      " +
                        "  'password' = '123456',                                    " +
                        "  'table-name' = 'test_source_union1'                       " +
                        ")");

        tbEnv.sqlQuery(
                "SELECT * FROM source1"
        ).execute().print();
    }


}
