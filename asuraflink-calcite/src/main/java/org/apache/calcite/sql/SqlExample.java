package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

// https://jxeditor.github.io/2020/07/03/Flink%E5%B0%8F%E6%89%A9%E5%B1%95%E4%B9%8BCalcite%E8%87%AA%E5%AE%9A%E4%B9%89SQL%E8%A7%A3%E6%9E%90%E5%99%A8/
public class SqlExample extends SqlNode {
    private String exampleString;
    private SqlParserPos pos;

    public SqlExample(SqlParserPos pos, String exampleString) {
        super(pos);
        this.pos = pos;
        this.exampleString = exampleString;
    }

    public String getExampleString() {
        System.out.println("getExampleString");
        return this.exampleString;
    }

    @Override
    public SqlNode clone(SqlParserPos sqlParserPos) {
        System.out.println("clone");
        return null;
    }

    @Override
    public void unparse(SqlWriter sqlWriter, int i, int i1) {
        sqlWriter.keyword("run");
        sqlWriter.keyword("example");
        sqlWriter.print("\n");
        sqlWriter.keyword(exampleString);
    }

    @Override
    public void validate(SqlValidator sqlValidator, SqlValidatorScope sqlValidatorScope) {
        System.out.println("validate");
    }

    @Override
    public <R> R accept(SqlVisitor<R> sqlVisitor) {
        System.out.println("validate");
        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode sqlNode, Litmus litmus) {
        System.out.println("equalsDeep");
        return false;
    }
}