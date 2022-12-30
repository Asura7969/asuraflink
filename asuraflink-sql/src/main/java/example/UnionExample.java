package example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * <pre>{@code
 * create table test_source_union1(a varchar(10) NOT NUll,b int(10) NOT NUll,c int(10) NOT NUll) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='source1';
 *
 * create table test_source_union2(a varchar(10) NOT NUll,b int(10) NOT NUll,c int(10) NOT NUll) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='source2';
 *
 *
 * insert into test_source_union1(a, b, c) values('test1', 1, 10);
 *
 * insert into test_source_union2(a, b, c) values('test1', 1, 10);
 * insert into test_source_union2(a, b, c) values('test2', 2, 20);
 * }</pre>
 */
public class UnionExample {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env);

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

        tbEnv.executeSql(
                "CREATE TABLE source2 (                                     " +
                        "  a STRING,                                                 " +
                        "  b INT,                                                    " +
                        "  c INT                                                     " +
                        ") WITH (                                                    " +
                        "  'connector' = 'jdbc',                                     " +
                        "  'url' = 'jdbc:mysql://localhost:3306/binlog',             " +
                        "  'username' = 'root',                                      " +
                        "  'password' = '123456',                                    " +
                        "  'table-name' = 'test_source_union2'                       " +
                        ")");
        String sql = "SELECT * FROM source1 UNION ALL SELECT * FROM source2";
        System.out.println(tbEnv.explainSql(sql));
        tbEnv.sqlQuery(sql).execute().print();
    }
}
