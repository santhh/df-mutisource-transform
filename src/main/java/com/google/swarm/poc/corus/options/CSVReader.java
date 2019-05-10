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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;

public class CSVReader extends DoFn<ReadableFile, Table> {

	private static final Logger LOG = LoggerFactory.getLogger(CSVReader.class);

	private ValueProvider<Integer> batchSize;
	/**
	 * This counter is used to track number of lines processed against batch size.
	 */
	private Integer lineCount;

	List<String> csvHeaders;

	public CSVReader(ValueProvider<Integer> batchSize) {
		lineCount = 1;
		this.batchSize = batchSize;
		// SourceSystem,LastUpdatedTime,Date,SiteTag,Salesperson,SalespersonId,Impressions,Clicks,Revenue,Type
		this.csvHeaders = Arrays.asList("SourceSystem", "LastUpdatedTime", "Date", "SiteTag", "Salesperson",
				"SalespersonId", "Impressions", "Clicks", "Revenue", "Type");
	}

	@ProcessElement
	public void processElement(ProcessContext c, OffsetRangeTracker tracker) throws IOException {
		for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {

			try (BufferedReader br = Util.getReader(c.element())) {

				if (csvHeaders != null) {
					List<FieldId> dlpTableHeaders = csvHeaders.stream()
							.map(header -> FieldId.newBuilder().setName(header).build()).collect(Collectors.toList());
					List<Table.Row> rows = new ArrayList<>();
					Table dlpTable = null;
					/** finding out EOL for this restriction so that we know the SOL */
					int endOfLine = (int) (i * batchSize.get().intValue());
					int startOfLine = (endOfLine - batchSize.get().intValue());
					/** skipping all the rows that's not part of this restriction */
					br.readLine();
					Iterator<CSVRecord> csvRows = CSVFormat.DEFAULT.withSkipHeaderRecord().parse(br).iterator();
					for (int line = 0; line < startOfLine; line++) {
						if (csvRows.hasNext()) {
							csvRows.next();
						}
					}
					/**
					 * looping through buffered reader and creating DLP Table Rows equals to batch
					 */
					while (csvRows.hasNext() && lineCount <= batchSize.get()) {

						CSVRecord csvRow = csvRows.next();
						rows.add(convertCsvRowToTableRow(csvRow));
						lineCount += 1;
					}
					/** creating DLP table and output for next transformation */
					dlpTable = Table.newBuilder().addAllHeaders(dlpTableHeaders).addAllRows(rows).build();
					c.output(dlpTable);

					LOG.info(
							"Current Restriction From: {}, Current Restriction To: {},"
									+ " StartofLine: {}, End Of Line {}, BatchData {}",
							tracker.currentRestriction().getFrom(), tracker.currentRestriction().getTo(), startOfLine,
							endOfLine, dlpTable.getRowsCount());

				} else {

					throw new RuntimeException("Header Values Can't be found For file Key ");
				}
			}
		}
	}

	/**
	 * SDF needs to define a @GetInitialRestriction method that can create a
	 * restriction describing the complete work for a given element. For our case
	 * this would be the total number of rows for each CSV file. We will calculate
	 * the number of split required based on total number of rows and batch size
	 * provided.
	 *
	 * @throws IOException
	 */
	@GetInitialRestriction
	public OffsetRange getInitialRestriction(ReadableFile csvFile) throws IOException {

		int rowCount = 0;
		int totalSplit = 0;
		try (BufferedReader br = Util.getReader(csvFile)) {
			/** assume first row is header */
			int checkRowCount = (int) br.lines().count() - 1;
			rowCount = (checkRowCount < 1) ? 1 : checkRowCount;
			totalSplit = rowCount / batchSize.get().intValue();
			int remaining = rowCount % batchSize.get().intValue();
			/**
			 * Adjusting the total number of split based on remaining rows. For example:
			 * batch size of 15 for 100 rows will have total 7 splits. As it's a range last
			 * split will have offset range {7,8}
			 */
			if (remaining > 0) {
				totalSplit = totalSplit + 2;

			} else {
				totalSplit = totalSplit + 1;
			}
		}

		LOG.debug("Initial Restriction range from 1 to: {}", totalSplit);
		return new OffsetRange(1, totalSplit);
	}

	/**
	 * SDF needs to define a @SplitRestriction method that can split the intital
	 * restricton to a number of smaller restrictions. For example: a intital
	 * rewstriction of (x, N) as input and produces pairs (x, 0), (x, 1), …, (x,
	 * N-1) as output.
	 */
	@SplitRestriction
	public void splitRestriction(ReadableFile csvFile, OffsetRange range, OutputReceiver<OffsetRange> out) {
		/** split the initial restriction by 1 */
		for (final OffsetRange p : range.split(1, 1)) {
			out.output(p);
		}
	}

	@NewTracker
	public OffsetRangeTracker newTracker(OffsetRange range) {
		return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));
	}

	private Table.Row convertCsvRowToTableRow(CSVRecord csvRow) {
		/** convert from CSV row to DLP Table Row */
		Iterator<String> valueIterator = csvRow.iterator();
		Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
		while (valueIterator.hasNext()) {
			String value = valueIterator.next();
			if (value != null) {
				tableRowBuilder.addValues(Value.newBuilder().setStringValue(value.toString()).build());
			} else {
				tableRowBuilder.addValues(Value.newBuilder().setStringValue("").build());
			}
		}

		return tableRowBuilder.build();
	}

}
