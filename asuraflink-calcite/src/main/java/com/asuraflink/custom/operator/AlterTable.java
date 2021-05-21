package com.asuraflink.custom.operator;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.List;

public class AlterTable extends SqlCall {
    /**
     * 修改的表
     */
    private SqlIdentifier oldTableName;

    /**
     * 新表名
     */
    private SqlIdentifier newTableName;

    /**
     * sqlParserPos
     */
    private SqlParserPos sqlParserPos;

    public AlterTable(SqlParserPos pos, SqlIdentifier oldTableName, SqlIdentifier newTableName) {
        super(pos);
        this.sqlParserPos = pos;
        this.oldTableName = oldTableName;
        this.newTableName = newTableName;
    }

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE) {
                @Override public AlterTable createCall(SqlLiteral functionQualifier,
                                                       SqlParserPos pos, SqlNode... operands) {
                    final SqlNode node1 = operands[0];
                    final SqlNode node2 = operands[0];
                    return new AlterTable(pos,(SqlIdentifier)node1,(SqlIdentifier)node2 );
                }
            };

    public AlterTable(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> list = new ArrayList<>();
        list.add(oldTableName);
        list.add(newTableName);
        return list;
    }

    @Override
    public void unparse(SqlWriter sqlWriter, int leftPrec, int rightPrec) {
        sqlWriter.keyword("ALTER");
        sqlWriter.keyword("TABLE");
        sqlWriter.print(oldTableName.toString());
        sqlWriter.keyword("RENAME");
        sqlWriter.keyword("TO");
        sqlWriter.print(newTableName.toString());
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        System.out.println("AlterTable clone");
        return null;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        System.out.println("AlterTable validate");
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        System.out.println("AlterTable equalsDeep");
        return false;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.ALTER_TABLE;
    }
}
