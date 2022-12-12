/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.json.user;

import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.*;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link UserRowDataSerializationSchema} and {@link UserRowDataDeserializationSchema}.
 */
public class UserJsonSerDeSchemaTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static final RowType SCHEMA = (RowType) ROW(
		FIELD("mail", STRING().notNull()),
		FIELD("guid", STRING().notNull()),
		FIELD("status", INT()),
		FIELD("ipAddress", STRING()),
		FIELD("metric", STRING()),
		FIELD("service", STRING()),
		FIELD("traceId", STRING()),
		FIELD("logValue", DOUBLE()),
		FIELD("time", STRING().notNull())
	).getLogicalType();

	@Test
	public void testFilteringTables() throws Exception {
		List<String> lines = readLines("user-data.txt");
		UserRowDataDeserializationSchema deserializationSchema = new UserRowDataDeserializationSchema(
			SCHEMA,
			InternalTypeInfo.of(SCHEMA),
			true,
			false,
			TimestampFormat.ISO_8601);
		runTest(lines, deserializationSchema);
	}

	@Test
	public void testSerializationDeserialization() throws Exception {
		List<String> lines = readLines("user-data.txt");

		UserRowDataDeserializationSchema deserializationSchema = new UserRowDataDeserializationSchema(
				SCHEMA,
				InternalTypeInfo.of(SCHEMA),
				true,
				false,
				TimestampFormat.ISO_8601);
		runTest(lines, deserializationSchema);
	}

	public void runTest(List<String> lines, UserRowDataDeserializationSchema deserializationSchema) throws Exception {
		SimpleCollector collector = new SimpleCollector();
		for (String line : lines) {
			deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
		}

		List<String> expected = Arrays.asList(
			"+I(zhangsan@qq.com,1111111,200,0.0.0.0,metric1,service1,385307967614d7c1,2.0,2020-08-06 17:20:24.066)",
			"+I(zhangsan@qq.com,1111111,200,0.0.0.0,metric1,service1,3asd7967614d7c12,3.0,2020-08-06 17:21:24.066)"
		);

		List<String> actual = collector.list.stream()
			.map(Object::toString)
			.collect(Collectors.toList());
		assertEquals(expected, actual);

		// test Serialization
		UserRowDataSerializationSchema serializationSchema = new UserRowDataSerializationSchema(
			SCHEMA,
			TimestampFormat.ISO_8601,
				JsonFormatOptions.MapNullKeyMode.LITERAL,
			"null"
		);
		serializationSchema.open(null);

		List<String> result = new ArrayList<>();
		for (RowData rowData : collector.list) {
			result.add(new String(serializationSchema.serialize(rowData), StandardCharsets.UTF_8));
		}

		List<String> expectedResult = Arrays.asList(
			"{\"mail\":\"zhangsan@qq.com\",\"guid\":\"1111111\",\"status\":200,\"ipAddress\":\"0.0.0.0\",\"metric\":\"metric1\",\"service\":\"service1\",\"traceId\":\"385307967614d7c1\",\"logValue\":2.0,\"time\":\"2020-08-06 17:20:24.066\"}",
			"{\"mail\":\"zhangsan@qq.com\",\"guid\":\"1111111\",\"status\":200,\"ipAddress\":\"0.0.0.0\",\"metric\":\"metric1\",\"service\":\"service1\",\"traceId\":\"3asd7967614d7c12\",\"logValue\":3.0,\"time\":\"2020-08-06 17:21:24.066\"}");

		assertEquals(expectedResult, result);
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	private static List<String> readLines(String resource) throws IOException {
		final URL url = UserJsonSerDeSchemaTest.class.getClassLoader().getResource(resource);
		assert url != null;
		Path path = new File(url.getFile()).toPath();
		return Files.readAllLines(path);
	}

	private static class SimpleCollector implements Collector<RowData> {

		private List<RowData> list = new ArrayList<>();

		@Override
		public void collect(RowData record) {
			list.add(record);
		}

		@Override
		public void close() {
			// do nothing
		}
	}
}
