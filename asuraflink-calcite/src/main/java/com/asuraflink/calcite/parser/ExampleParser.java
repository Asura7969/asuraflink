package com.asuraflink.calcite.parser;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
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
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder()
                        .setParserFactory(SqlParserImpl.FACTORY)
                        .setCaseSensitive(false)
                        .setQuoting(Quoting.BACK_TICK)
                        .setQuotedCasing(Casing.TO_UPPER)
                        .setUnquotedCasing(Casing.TO_UPPER)
                        .setConformance(SqlConformanceEnum.ORACLE_12)
                        .build())
                .build();

        String sql = "select ids, name from test where id < 5 and name = 'zhang'";
        SqlParser parser = SqlParser.create(sql, config.getParserConfig());
        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println(sqlNode.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
