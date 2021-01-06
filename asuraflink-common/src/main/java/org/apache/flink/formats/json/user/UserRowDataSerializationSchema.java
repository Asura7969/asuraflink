package org.apache.flink.formats.json.user;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.formats.json.TimestampFormat;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class UserRowDataSerializationSchema implements SerializationSchema<RowData> {
    private final JsonRowDataSerializationSchema jsonSerializer;
    private transient GenericRowData reuse;
    private final RowType rowType;

    public UserRowDataSerializationSchema(
            RowType rowType,
            TimestampFormat timestampFormat,
            JsonOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral) {
        this.rowType = checkNotNull(rowType);
        jsonSerializer = new JsonRowDataSerializationSchema(
                rowType,
                timestampFormat,
                mapNullKeyMode,
                mapNullKeyLiteral);
    }

    @Override
    public void open(InitializationContext context) {
        reuse = new GenericRowData(rowType.getFieldCount());
    }

    @Override
    public byte[] serialize(RowData row) {
        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            reuse.setField(i, ((GenericRowData) row).getField(i));
        }
        return jsonSerializer.serialize(reuse);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserRowDataSerializationSchema that = (UserRowDataSerializationSchema) o;
        return Objects.equals(jsonSerializer, that.jsonSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jsonSerializer);
    }
}
