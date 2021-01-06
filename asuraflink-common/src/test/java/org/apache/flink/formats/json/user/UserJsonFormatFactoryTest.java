/*
package org.apache.flink.formats.json.user;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class UserJsonFormatFactoryTest extends TestLogger {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static final TableSchema SCHEMA = TableSchema.builder()
		.field("field1", DataTypes.BOOLEAN())
		.field("field2", DataTypes.INT())
		.build();

	private static final RowType ROW_TYPE = (RowType) SCHEMA.toRowDataType().getLogicalType();

	@Test
	public void testSeDeSchema() {
		final Map<String, String> tableOptions = getAllOptions();

		testSchemaSerializationSchema(tableOptions);

		testSchemaDeserializationSchema(tableOptions);
	}

	private void testSchemaDeserializationSchema(Map<String, String> options) {
		final UserRowDataDeserializationSchema expectedDeser =
			new UserRowDataDeserializationSchema(
				ROW_TYPE,
				InternalTypeInfo.of(ROW_TYPE),
				false,
				true,
				TimestampFormat.ISO_8601);

		final DynamicTableSource actualSource = createTableSource(options);
		assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
		TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
			(TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

		DeserializationSchema<RowData> actualDeser = scanSourceMock.valueFormat
			.createRuntimeDecoder(
				ScanRuntimeProviderContext.INSTANCE,
				SCHEMA.toRowDataType());

		assertEquals(expectedDeser, actualDeser);
	}

	private static DynamicTableSource createTableSource(Map<String, String> options) {
		return FactoryUtil.createTableSource(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new CatalogTableImpl(SCHEMA, options, "Mock scan table"),
			new Configuration(),
			UserJsonFormatFactoryTest.class.getClassLoader(),
			false);
	}

	private void testSchemaSerializationSchema(Map<String, String> options) {
		final UserRowDataSerializationSchema expectedSer = new UserRowDataSerializationSchema(
			ROW_TYPE,
			TimestampFormat.ISO_8601,
			JsonOptions.MapNullKeyMode.LITERAL,
			"null");

		final DynamicTableSink actualSink = createTableSink(options);
		assert actualSink instanceof TestDynamicTableFactory.DynamicTableSinkMock;
		TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
			(TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

		SerializationSchema<RowData> actualSer = sinkMock.valueFormat
			.createRuntimeEncoder(
				new SinkRuntimeProviderContext(false),
				SCHEMA.toRowDataType());

		assertEquals(expectedSer, actualSer);
	}

	private Map<String, String> getAllOptions() {
		final Map<String, String> options = new HashMap<>();
		options.put("connector", TestDynamicTableFactory.IDENTIFIER);
		options.put("target", "MyTarget");
		options.put("buffer-size", "1000");

		options.put("format", UserJsonFormatFactory.IDENTIFIER);
		options.put("user-json.fail-on-missing-field", "false");
		options.put("user-json.ignore-parse-errors", "true");
		options.put("user-json.timestamp-format.standard", "ISO-8601");
		options.put("user-json.map-null-key.mode", "LITERAL");
		options.put("user-json.map-null-key.literal", "null");
		return options;
	}

	private static DynamicTableSink createTableSink(Map<String, String> options) {
		return FactoryUtil.createTableSink(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new CatalogTableImpl(SCHEMA, options, "Mock sink table"),
			new Configuration(),
			UserJsonFormatFactoryTest.class.getClassLoader(),
			false);
	}
}
*/
