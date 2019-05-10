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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

public interface DataProcessorPipelineOptions extends DataflowPipelineOptions {

	@Description("The file pattern to read records from (e.g. gs://bucket/file-*.csv)")
	ValueProvider<String> getInputFilePattern();

	void setInputFilePattern(ValueProvider<String> value);

	@Description("DLP Deidentify Template to be used for API request "
			+ "(e.g.projects/{project_id}/deidentifyTemplates/{deIdTemplateId}")
	@Required
	ValueProvider<String> getDeidentifyTemplateName();

	void setDeidentifyTemplateName(ValueProvider<String> value);

	@Description("DLP Inspect Template to be used for API request "
			+ "(e.g.projects/{project_id}/inspectTemplates/{inspectTemplateId}")
	ValueProvider<String> getInspectTemplateName();

	void setInspectTemplateName(ValueProvider<String> value);

	@Description("DLP API has a limit for payload size of 524KB /api call. "
			+ "That's why dataflow process will need to chunk it. User will have to decide "
			+ "on how they would like to batch the request depending on number of rows " + "and how big each row is.")
	@Required
	ValueProvider<Integer> getBatchSize();

	void setBatchSize(ValueProvider<Integer> value);

	@Description("Big Query data set must exist before the pipeline runs (e.g. pii-dataset")
	ValueProvider<String> getTableSpec();

	void setTableSpec(ValueProvider<String> value);

	@Description("Path of the file to write to")
	ValueProvider<String> getOutputFile();

	void setOutputFile(ValueProvider<String> value);

	@Description("Subscribtion to read from")
	ValueProvider<String> getSubTopic();

	void setSubTopic(ValueProvider<String> value);

	@Description("JDBC Spec")
	ValueProvider<String> getJDBCSpec();

	void setJDBCSpec(ValueProvider<String> value);

	@Description("API url")
	ValueProvider<String> getApiUrl();

	void setApiUrl(ValueProvider<String> value);

}
