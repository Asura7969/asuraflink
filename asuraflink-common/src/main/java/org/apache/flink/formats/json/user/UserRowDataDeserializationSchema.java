package org.apache.flink.formats.json.user;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.TimestampFormat;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.activation.UnsupportedDataTypeException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class UserRowDataDeserializationSchema implements DeserializationSchema<RowData> {
    private final boolean failOnMissingField;
    private final boolean ignoreParseErrors;
    private final TypeInformation<RowData> resultTypeInfo;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TimestampFormat timestampFormat;
    private final RowType rowType;

    public UserRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new IllegalArgumentException(
                    "YUKON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.rowType = checkNotNull(rowType);
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
        boolean hasDecimalType = LogicalTypeChecks.hasNested(
                rowType,
                t -> t instanceof DecimalType);
        if (hasDecimalType) {
            objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        }
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        try {
            int startIndex = 0;
            for (int i = 0; i < message.length; i++) {
                if (message[i] == '{') {
                    startIndex = i;
                    break;
                }
            }
            byte[] headBytes = Arrays.copyOfRange(message, 0, startIndex);
            String time = new String(headBytes, StandardCharsets.UTF_8).split(",")[1];
            byte[] jsonBytes = Arrays.copyOfRange(message, startIndex, message.length);
            final JsonNode root = objectMapper.readTree(jsonBytes);
            List<RowType.RowField> fields = rowType.getFields();

            GenericRowData data = new GenericRowData(fields.size());

            for (int i = 0; i < fields.size(); i++) {
                RowType.RowField field = fields.get(i);
                String fieldName = field.getName();
                if (fieldName.equals("time")) {
                    data.setField(fields.size() - 1, StringData.fromString(time));
                } else {
                    JsonNode value = root.findValue(fieldName);
                    if (value.isContainerNode()) {
                        throw new UnsupportedDataTypeException("Unsupported type: " + fieldName);
                    }
                    data.setField(i, getValueFromJsonNode(value, field.getType().getTypeRoot()));
                }
            }
            return data;
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            throw new IOException(
                    format("Failed to deserialize JSON '%s'.", new String(message)), t);
        }
    }

    public Object getValueFromJsonNode(JsonNode jsonNode, LogicalTypeRoot type) {
        switch (type) {
            case CHAR:
            case VARCHAR:
                return StringData.fromString(jsonNode.asText());
            case BOOLEAN:
                return jsonNode.asBoolean();
            case INTEGER:
                return jsonNode.asInt();
            case FLOAT:
            case DOUBLE:
                return jsonNode.asDouble();
            case BIGINT:
                return jsonNode.asLong();
            default:
                return null;
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }


    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserRowDataDeserializationSchema that = (UserRowDataDeserializationSchema) o;
        return failOnMissingField == that.failOnMissingField &&
                ignoreParseErrors == that.ignoreParseErrors &&
                resultTypeInfo.equals(that.resultTypeInfo) &&
                timestampFormat.equals(that.timestampFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(failOnMissingField, ignoreParseErrors, resultTypeInfo, timestampFormat);
    }
}
