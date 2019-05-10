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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.privacy.dlp.v2.Table;

@SuppressWarnings("serial")
public class TableRowProcessorDoFn extends DoFn<Table, TableRow> {

	@ProcessElement
	public void processElement(ProcessContext c) {

		Table tokenizedData = c.element();
		List<String> headers = tokenizedData.getHeadersList().stream().map(fid -> fid.getName())
				.collect(Collectors.toList());

		List<Table.Row> outputRows = tokenizedData.getRowsList();
		if (outputRows.size() > 0) {
			for (Table.Row outputRow : outputRows) {
				c.output(Util.createBqRow(outputRow, headers.toArray(new String[headers.size()])));
			}
		}
	}

}
