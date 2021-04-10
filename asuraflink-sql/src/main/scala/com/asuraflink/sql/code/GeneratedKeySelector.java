package com.asuraflink.sql.code;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.runtime.generated.GeneratedClass;

/**
 * @author asura7969
 * @create 2021-04-10-20:31
 */
public class GeneratedKeySelector <K extends KeySelector<?,?>> extends GeneratedClass<K> {
    private static final long serialVersionUID = 2153604577461978440L;

    protected GeneratedKeySelector(String className, String code, Object[] references) {
        super(className, code, references);
    }
}
