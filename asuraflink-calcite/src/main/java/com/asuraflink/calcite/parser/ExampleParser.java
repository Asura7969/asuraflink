package com.asuraflink.calcite.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.ExampleSqlParserImpl;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

/**
 * @author asura7969
 * @create 2021-04-25-23:45
 */
public class ExampleParser {

    public static void main(String[] args) {
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder()
                        .setParserFactory(ExampleSqlParserImpl.FACTORY)
                        // 设置大小写是否敏感
                        .setCaseSensitive(false)
                        // 设置应用标识,mysql是``
                        .setQuoting(Quoting.BACK_TICK)
                        // Quoting策略,不变,变大写或变小写
                        .setQuotedCasing(Casing.TO_UPPER)
                        // 标识符没有被Quoting后的策略
                        .setUnquotedCasing(Casing.TO_UPPER)
                        .build())
                .build();
        String sql = "run example 'select ids, name from test where id < 5'";
        SqlParser parser = SqlParser.create(sql, config.getParserConfig());
        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println(sqlNode.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
