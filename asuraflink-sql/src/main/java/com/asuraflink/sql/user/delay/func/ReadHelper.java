package com.asuraflink.sql.user.delay.func;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.TypeInference;

/**
 * 资源连接接口，如jdbc，hbase，redis等(必须并发安全的连接)
 *
 * @param <T>
 */
public abstract class ReadHelper<IN, OUT> {
    abstract public void open();

    abstract public void close();

    public OUT eval(IN o) {
        return null;
    }

    public OUT eval(IN... values) {
        return null;
    }

    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return null;
    }

    @Deprecated
    public TypeInformation<OUT> getResultType() {
        return null;
    }

    @Deprecated
    public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
        return null;
    }
}
