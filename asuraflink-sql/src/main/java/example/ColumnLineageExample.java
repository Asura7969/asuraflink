package example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
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
 *
 * create table test_sink(a1 varchar(10) NOT NUll, a varchar(10) NOT NUll,b int(10) NOT NUll,c int(10) NOT NUll) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='test_sink';
 * }</pre>
 */
public class ColumnLineageExample {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        tbEnv.executeSql(
                "CREATE TABLE mysql_sink (                                  " +
                        "  a1 STRING,                                                " +
                        "  a STRING,                                                 " +
                        "  b INT,                                                    " +
                        "  c INT                                                     " +
                        ") WITH (                                                    " +
                        "  'connector' = 'jdbc',                                     " +
                        "  'url' = 'jdbc:mysql://localhost:3306/binlog',             " +
                        "  'username' = 'root',                                      " +
                        "  'password' = '123456',                                    " +
                        "  'table-name' = 'test_sink'                                " +
                        ")");

        String sql = "INSERT INTO mysql_sink " +
                "SELECT  s2.a, s1.a as aa, s2.b, s2.c FROM source1 as s1 INNER JOIN source2 as s2 on s2.a = s1.a";

        System.out.println(tbEnv.explainSql(sql));
        TableResult tableResult = tbEnv.executeSql(sql);
        tableResult.print();
    }
}
