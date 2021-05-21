package com.asuraflink.calcite.parser;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.ExampleSqlParserImpl;

import java.util.List;

public class AlterTableTest {

    public static void main(String[] args) throws SqlParseException {

        SqlParser.Config mysqlConfig = SqlParser.configBuilder()
                // 定义解析工厂
                .setParserFactory(ExampleSqlParserImpl.FACTORY)
                .setLex(Lex.MYSQL)
                .build();
        // 创建解析器
        SqlParser parser = SqlParser.create("", mysqlConfig);
        // Sql语句
        String sql = "ALTER TABLE dev.console_component RENAME TO chener.console_component_bak ";

        String sql2 = "ALTER TABLE console_component RENAME TO console_component_bak ";

        String sql3 = "ALTER console_component  RENAME TO console_component_bak ";

        String sql4 = "select * from chener.dev";

        // 解析sql
        SqlNode sqlNode = parser.parseQuery(sql);
        SqlKind kind = sqlNode.getKind();
        if (sqlNode instanceof SqlCall) {
            List<SqlNode> operandList = ((SqlCall) sqlNode).getOperandList();
            operandList.forEach(System.out::println);
        }
        System.out.println(kind);
        // 还原某个方言的SQL
        System.out.println(sqlNode.toSqlString(OracleSqlDialect.DEFAULT));
    }
}
