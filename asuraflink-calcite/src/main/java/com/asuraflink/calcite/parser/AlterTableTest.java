package com.asuraflink.calcite.parser;

import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.ExampleSqlParserImpl;

import java.util.List;
import java.util.stream.Collectors;

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

        String sql5 = "select /*+ side_out_put('outputTagName1'),side_out_put('outputTagName2') */ * from chener.dev";

        HintStrategyTable side_out_put = HintStrategyTable.builder()
                .hintStrategy("side_out_put", HintPredicates.PROJECT).build();
        // 解析sql
        SqlNode sqlNode = parser.parseQuery(sql5);
        SqlKind kind = sqlNode.getKind();
        if (sqlNode instanceof SqlCall) {
            List<SqlNode> operandList = ((SqlCall) sqlNode).getOperandList();

            if (!operandList.isEmpty()) {
                operandList.forEach(node -> {
                    if (node instanceof SqlNodeList) {
                        if (((SqlNodeList) node).size() != 0) {
                            boolean b = ((SqlNodeList) node).getList().stream().anyMatch(x -> x instanceof SqlHint);
                            if (b) {
                                List<RelHint> relHint = SqlUtil.getRelHint(side_out_put, (SqlNodeList) node);

                                List<List<String>> side_out_put1 = relHint.stream().filter(hint -> hint.hintName.equalsIgnoreCase("side_out_put"))
                                        .map(hint -> hint.listOptions).collect(Collectors.toList());
                                System.out.println(side_out_put1);
                            }

                        }

                    }
                });
//                operandList.forEach(System.out::println);
            }
        }
//        System.out.println(kind);
        // 还原某个方言的SQL
//        System.out.println(sqlNode.toSqlString(OracleSqlDialect.DEFAULT));
    }
}
