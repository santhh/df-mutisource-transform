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
package com.google.swarm.poc.corus;

import java.nio.charset.StandardCharsets;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.swarm.poc.corus.options.CSVReader;
import com.google.swarm.poc.corus.options.DLPTokenizationDoFn;
import com.google.swarm.poc.corus.options.DataProcessorPipelineOptions;
import com.google.swarm.poc.corus.options.TableRowProcessorDoFn;
import com.google.swarm.poc.corus.options.Util;

public class DataProcessorPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(DataProcessorPipeline.class);
	private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(600);
	private static final String JSON_SCHEMA_RAW_TABLE = "etl_schema.json";
	private static final String JSON_SCHEMA_AGGR_TABLE = "aggr_schema.json";
	private static final String AGGR_TABLE = "corus-data-science-poc:corus_digital_data.corus_aggregation";
	private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(30);

	public static void main(String args[]) {

		DataProcessorPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(DataProcessorPipelineOptions.class);
		RestTemplate restTemplate = new RestTemplate();
		final HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		headers.set("X-API-Key", "sampleapikey");
		UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(options.getApiUrl().get());
		LOG.info("API URL {}", builder.buildAndExpand().toUri());
		String response = restTemplate
				.exchange(builder.buildAndExpand().toUri(), HttpMethod.GET, new HttpEntity<>(headers), String.class)
				.getBody();

		run(options, response);

	}

	@SuppressWarnings("serial")
	public static PipelineResult run(DataProcessorPipelineOptions options, String response) {

		Pipeline p = Pipeline.create(options);
		PCollection<TableRow> apiDataCollection = p.apply("CPAX Data From REST API", Create.of(response))
				.apply("API Converts", ParDo.of(new DoFn<String, TableRow>() {

					@ProcessElement
					public void processElement(ProcessContext c) {

						JsonParser parser = new JsonParser();
						JsonArray elements = parser.parse(c.element()).getAsJsonArray();

						elements.forEach(element -> {
							JsonObject obj = element.getAsJsonObject();
							c.output(Util.convertJsonToTableRow(Util.modifyJson(obj, Util.CPAX_API)));
						});
					}
				}));

		// pub sub
		PCollection<TableRow> pubSubDataCollection = p
				.apply("ADAX Data From (Pub/Sub)",
						PubsubIO.readMessagesWithAttributes().fromSubscription(options.getSubTopic()))
				.apply("PubSub Converts", MapElements.via(new SimpleFunction<PubsubMessage, TableRow>() {
					@Override
					public TableRow apply(PubsubMessage json) {
						
						JsonParser parser = new JsonParser();
						JsonObject element = parser.parse(new String(json.getPayload(), StandardCharsets.UTF_8))
								.getAsJsonObject();
						return Util.convertJsonToTableRow(Util.modifyJson(element, Util.ADAX_API));
					}
				}));

		// gcs
		PCollection<TableRow> gcsCollection = p
				.apply("Find-MonetizableImpressions Files",
						FileIO.match().filepattern(options.getInputFilePattern()).continuously(DEFAULT_POLL_INTERVAL,
								Watch.Growth.never()))
				.apply("FindCSVFiles", FileIO.readMatches().withCompression(Compression.AUTO))
				.apply("Process-MonetizableImpressions", ParDo.of(new CSVReader(options.getBatchSize())))
				.apply("PII-DataInspection",
						ParDo.of(new DLPTokenizationDoFn(options.getProject(), options.getDeidentifyTemplateName(),
								options.getInspectTemplateName())))
				.apply("GCS Monetizable Impressions Converts", ParDo.of(new TableRowProcessorDoFn()));
		// jdbc
		PCollection<TableRow> dbCollection = p.apply("DistrictM(Cloud SQL)Converts", JdbcIO.<TableRow>read()
				.withDataSourceConfiguration(
						JdbcIO.DataSourceConfiguration.create("com.mysql.jdbc.Driver", options.getJDBCSpec().get()))
				.withQuery("select * from district_m").withCoder(TableRowJsonCoder.of())
				.withRowMapper(Util.getResultSetToTableRow()));

		PCollection<TableRow> dataCollections = PCollectionList.of(apiDataCollection).and(pubSubDataCollection)
				.and(gcsCollection).and(dbCollection).apply("JoinData-ALL Sources", Flatten.<TableRow>pCollections());

		dataCollections.apply("Write to BQ Table (Raw Data)",
				BigQueryIO.writeTableRows().to(options.getTableSpec()).withoutValidation()
						.withCreateDisposition(CreateDisposition.CREATE_NEVER)
						.withWriteDisposition(WriteDisposition.WRITE_APPEND).withoutValidation()
						.withJsonSchema(Util.getJsonTableSchema(JSON_SCHEMA_RAW_TABLE)));

		PCollection<TableRow> aggregateData = dataCollections
				.apply("Add Site Tag as Key", WithKeys.of(row -> row.get("SiteTag").toString()))
				.setCoder(KvCoder.of(StringUtf8Coder.of(), TableRowJsonCoder.of()))

				.apply("Fixed Window",
						Window.<KV<String, TableRow>>into(FixedWindows.of(WINDOW_INTERVAL))
								.triggering(AfterWatermark.pastEndOfWindow()
										.withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
												.plusDelayOf(Duration.standardSeconds(1)))
										.withLateFirings(AfterPane.elementCountAtLeast(1)))
								.discardingFiredPanes().withAllowedLateness(Duration.ZERO))
				.apply("Group By Key", GroupByKey.create())
				.apply("Count Tag", ParDo.of(new DoFn<KV<String, Iterable<TableRow>>, KV<String, Long>>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						Long count = StreamSupport.stream(c.element().getValue().spliterator(), false).count();
						LOG.info("Tag {}, Count {}", c.element().getKey(), count);
						c.output(KV.of(c.element().getKey(), count));
					}

				})).apply("BQ Row Convert", ParDo.of(new DoFn<KV<String, Long>, TableRow>() {

					@ProcessElement
					public void processElement(ProcessContext c) {

						LOG.debug("Tag {} Count{} ", c.element().getKey(), c.element().getValue());
						TableRow row = new TableRow();
						row.set("SiteTag", c.element().getKey());
						row.set("TagCount", c.element().getValue());
						row.set("LastUpdatedTimestamp", Util.getTimestamp());
						c.output(row);

					}

				}));

		aggregateData.apply("Write to BQ Table (Aggregate Data)",
				BigQueryIO.writeTableRows().to(AGGR_TABLE).withoutValidation()
						.withCreateDisposition(CreateDisposition.CREATE_NEVER)
						.withWriteDisposition(WriteDisposition.WRITE_APPEND).withoutValidation()
						.withJsonSchema(Util.getJsonTableSchema(JSON_SCHEMA_AGGR_TABLE)));

		return p.run();

	}
}
