/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.swarm.poc.corus.options;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.privacy.dlp.v2.Table;

public class Util {
	private static final Logger LOG = LoggerFactory.getLogger(Util.class);
	public static final String CPAX_API = "CPAX_API";
	public static final String ADAX_API = "ADAX_EVENTS";
	private static final String SOURCE_FIELD_NAME = "SourceSystem";
	private static final String TIMESTAMP_FIELD_NAME = "LastUpdatedTime";
	private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
	public static JsonParser parser = new JsonParser();
	public static final String COLUMN_NAME_REGEXP = "^[A-Za-z_]+[A-Za-z_0-9]*$";

	public static String getTimestamp() {
		return TIMESTAMP_FORMATTER.print(Instant.now().toDateTime(DateTimeZone.UTC));

	}

	public static String modifyJson(JsonObject obj, String source) {

		obj.addProperty(SOURCE_FIELD_NAME, source);
		obj.addProperty(TIMESTAMP_FIELD_NAME, getTimestamp());
		return obj.toString();

	}

	public static TableRow convertJsonToTableRow(String json) {
		TableRow row;
		try (InputStream inputStream = new ByteArrayInputStream(json.trim().getBytes(StandardCharsets.UTF_8))) {

			row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);

		} catch (IOException e) {
			LOG.error("Can't parse JSON message {} JSON message {}", e.toString(), json);
			throw new RuntimeException("Failed to serialize json to table row: " + e.getMessage());
		}

		return row;
	}

	public static BufferedReader getReader(ReadableFile csvFile) {
		BufferedReader br = null;
		ReadableByteChannel channel = null;
		/** read the file and create buffered reader */
		try {
			channel = csvFile.openSeekable();

		} catch (IOException e) {
			LOG.error("Failed to Read File {}", e.getMessage());
			throw new RuntimeException(e);
		}

		if (channel != null) {

			br = new BufferedReader(Channels.newReader(channel, Charsets.ISO_8859_1.name()));
		}

		return br;
	}

	public static JdbcIO.RowMapper<TableRow> getResultSetToTableRow() {
		return new ResultSetToTableRow();
	}

	public static class ResultSetToTableRow implements JdbcIO.RowMapper<TableRow> {

		@Override
		public TableRow mapRow(ResultSet resultSet) throws Exception {

			ResultSetMetaData metaData = resultSet.getMetaData();

			TableRow outputTableRow = new TableRow();
			String timestamp = getTimestamp();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				outputTableRow.set(metaData.getColumnName(i), resultSet.getObject(i));
				outputTableRow.set(SOURCE_FIELD_NAME, "CloudSQL-DistrictM");
				outputTableRow.set(TIMESTAMP_FIELD_NAME, timestamp);

			}

			return outputTableRow;
		}
	}

	public static String getJsonTableSchema(String jsonSchemaFileName) {
		String schemaJson = null;
		try {
			schemaJson = Resources.toString(Resources.getResource(jsonSchemaFileName), StandardCharsets.UTF_8);
		} catch (Exception e) {
			LOG.error("Unable to read {} file from the resources folder!", jsonSchemaFileName, e);
		}

		return schemaJson;
	}

	public static TableRow createBqRow(Table.Row tokenizedValue, String[] headers) {
		TableRow bqRow = new TableRow();
		String dlpRow = tokenizedValue.getValuesList().stream().map(value -> value.getStringValue())
				.collect(Collectors.joining(","));
		String[] values = dlpRow.split(",");
		String timestamp = getTimestamp();
		for (int i = 0; i < headers.length; i++) {

			if (i == 1) {
				bqRow.set(headers[i].toString(), timestamp);
			} else {
				bqRow.set(headers[i].toString(), values[i].toString());
			}

		}

		return bqRow;
	}

}
