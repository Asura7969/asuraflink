package org.apache.flink.formats.json.user;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.formats.json.JsonOptions.FAIL_ON_MISSING_FIELD;
import static org.apache.flink.formats.json.JsonOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.json.JsonOptions.MAP_NULL_KEY_LITERAL;
import static org.apache.flink.formats.json.JsonOptions.MAP_NULL_KEY_MODE;
import static org.apache.flink.formats.json.JsonOptions.MapNullKeyMode;
import static org.apache.flink.formats.json.JsonOptions.TIMESTAMP_FORMAT;
import static org.apache.flink.formats.json.JsonOptions.validateEncodingFormatOptions;
import static org.apache.flink.formats.json.debezium.DebeziumJsonOptions.validateDecodingFormatOptions;

public class UserJsonFormatFactory implements
        DeserializationFormatFactory,
        SerializationFormatFactory {

    public static final String IDENTIFIER = "user-json";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context,
            ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateDecodingFormatOptions(formatOptions);

        final boolean failOnMissingField = formatOptions.get(FAIL_ON_MISSING_FIELD);
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        TimestampFormat timestampOption = JsonOptions.getTimestampFormat(formatOptions);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context,
                    DataType producedDataType) {
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(producedDataType);
                return new UserRowDataDeserializationSchema(
                        rowType,
                        rowDataTypeInfo,
                        failOnMissingField,
                        ignoreParseErrors,
                        timestampOption
                );
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context,
            ReadableConfig formatOptions) {

        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateEncodingFormatOptions(formatOptions);

        TimestampFormat timestampOption = JsonOptions.getTimestampFormat(formatOptions);
        MapNullKeyMode mapNullKeyMode = JsonOptions.getMapNullKeyMode(formatOptions);
        String mapNullKeyLiteral = formatOptions.get(MAP_NULL_KEY_LITERAL);

        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context,
                    DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                return new UserRowDataSerializationSchema(
                        rowType,
                        timestampOption,
                        mapNullKeyMode,
                        mapNullKeyLiteral);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FAIL_ON_MISSING_FIELD);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(TIMESTAMP_FORMAT);
        options.add(MAP_NULL_KEY_MODE);
        options.add(MAP_NULL_KEY_LITERAL);
        return options;
    }
}
