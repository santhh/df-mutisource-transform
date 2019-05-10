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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

public class JSONEventPublisher {
	
	private static final Logger LOG = LoggerFactory.getLogger(JSONEventPublisher.class);

	public static void main (String args[]) {
		Pipeline pipeline = Pipeline.create();

		PCollection<ReadableFile> jsonFile = pipeline
				.apply("Poll Input Files",
						FileIO.match().filepattern("gs://corus-poc-data/source/adx_newline_updated.json")
						.continuously(Duration.standardSeconds(30),
								Watch.Growth.never()))
				.apply("Find Pattern Match", FileIO.readMatches()
						.withCompression(Compression.UNCOMPRESSED));

		//Publish to PubSub
				jsonFile.apply("File Processor", ParDo.of(new DoFn<ReadableFile, PubsubMessage>() {

					@ProcessElement
					public void processElement(ProcessContext c) throws IOException {
						ReadableFile file = c.element();
						 AtomicInteger counter = new AtomicInteger(0);
							try (BufferedReader br = getReader(file)) {
				              
							  
				               br.lines().forEach(line->{
									LOG.info("Json {}",line.toString());
									PubsubMessage message = new PubsubMessage(line.getBytes(), null);
									counter.getAndIncrement();
									c.output(message);
								});
								
								LOG.info("Successfully Published {} ", counter);
								
			                   } catch (IOException e) {
			                     LOG.error("Failed to Read File {}", e.getMessage());
			                     throw new RuntimeException(e);
			                   }	
							
						} 
							
					
						

				})).apply("Publish Events", PubsubIO.writeMessages()
						.withMaxBatchBytesSize(100000).withMaxBatchSize(1000)
						.to("projects/corus-data-science-poc/topics/events"));

		pipeline.run().waitUntilFinish();
	}
	



private static BufferedReader getReader(ReadableFile eventFile) {
    BufferedReader br = null;
    ReadableByteChannel channel = null;
    /** read the file and create buffered reader */
    try {
      channel = eventFile.openSeekable();

    } catch (IOException e) {
      LOG.error("Failed to Open File {}", e.getMessage());
      throw new RuntimeException(e);
    }

    if (channel != null) {

      br = new BufferedReader(Channels.newReader(channel, Charsets.ISO_8859_1.name()));
    }

    return br;
  }


}
