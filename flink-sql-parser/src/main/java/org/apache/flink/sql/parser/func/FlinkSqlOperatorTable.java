package org.apache.flink.sql.parser.func;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlGroupedWindowFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;

import java.util.List;

public class FlinkSqlOperatorTable {

    public static final SqlGroupedWindowFunction INCREMENT_OLD =
            new SqlGroupedWindowFunction(
                    "$INCREMENT",
                    SqlKind.OTHER_FUNCTION,
                    null,
                    OperandTypes.or(
                            OperandTypes.DATETIME_INTERVAL_INTERVAL, OperandTypes.DATETIME_INTERVAL_INTERVAL_TIME)) {
                @Override
                public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
                    return ImmutableList.of(INCREMENT_START);
                }
            };

    public static final SqlGroupedWindowFunction INCREMENT_START =
            INCREMENT_OLD.auxiliary("INCREMENT_START", SqlKind.OTHER_FUNCTION);

}
