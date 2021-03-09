package com.asuraflink.sql.user.delay.func;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.TypeInference;

/**
 * 资源连接接口，如jdbc，hbase，redis等(必须并发安全的连接)
 *
 * @param <T>
 */
public interface ReadHelper<T> {
    public void open();

    public void close();

    public T eval(Object o);

    public T eval(Object... values);

    public TypeInference getTypeInference(DataTypeFactory typeFactory);

    @Deprecated
    public TypeInformation<T> getResultType();

    @Deprecated
    public TypeInformation<?>[] getParameterTypes(Class<?>[] signature);
}
