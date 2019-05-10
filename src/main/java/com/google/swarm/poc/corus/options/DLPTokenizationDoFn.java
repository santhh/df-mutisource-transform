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

import java.io.IOException;
import java.sql.SQLException;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentRequest.Builder;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;

@SuppressWarnings("serial")
public class DLPTokenizationDoFn extends DoFn<Table, Table> {
	private static final Logger LOG = LoggerFactory.getLogger(DLPTokenizationDoFn.class);

	private String dlpProjectId;
	private DlpServiceClient dlpServiceClient;
	private ValueProvider<String> deIdentifyTemplateName;
	private ValueProvider<String> inspectTemplateName;
	private boolean inspectTemplateExist;
	private Builder requestBuilder;

	public DLPTokenizationDoFn(String dlpProjectId, ValueProvider<String> deIdentifyTemplateName,
			ValueProvider<String> inspectTemplateName) {
		this.dlpProjectId = dlpProjectId;
		this.dlpServiceClient = null;
		this.deIdentifyTemplateName = deIdentifyTemplateName;
		this.inspectTemplateName = inspectTemplateName;
		this.inspectTemplateExist = false;
	}

	@Setup
	public void setup() {
		if (this.inspectTemplateName.isAccessible()) {
			if (this.inspectTemplateName.get() != null) {
				this.inspectTemplateExist = true;
			}
		}
		if (this.deIdentifyTemplateName.isAccessible()) {
			if (this.deIdentifyTemplateName.get() != null) {
				this.requestBuilder = DeidentifyContentRequest.newBuilder()
						.setParent(ProjectName.of(this.dlpProjectId).toString())
						.setDeidentifyTemplateName(this.deIdentifyTemplateName.get());
				if (this.inspectTemplateExist) {
					this.requestBuilder.setInspectTemplateName(this.inspectTemplateName.get());
				}
			}
		}
	}

	@StartBundle
	public void startBundle() throws SQLException {

		try {
			this.dlpServiceClient = DlpServiceClient.create();

		} catch (IOException e) {
			LOG.error("Failed to create DLP Service Client", e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@FinishBundle
	public void finishBundle() throws Exception {
		if (this.dlpServiceClient != null) {
			this.dlpServiceClient.close();
		}
	}

	@ProcessElement
	public void processElement(ProcessContext c) {

		Table nonEncryptedData = c.element();
		ContentItem tableItem = ContentItem.newBuilder().setTable(nonEncryptedData).build();
		this.requestBuilder.setItem(tableItem);
		DeidentifyContentResponse response = dlpServiceClient.deidentifyContent(this.requestBuilder.build());
		Table tokenizedData = response.getItem().getTable();

		c.output(tokenizedData);
	}
}
