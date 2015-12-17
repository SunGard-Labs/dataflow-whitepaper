package com.sungard.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class SymbolTransformPipeline {
	private static final Logger LOGGER = LoggerFactory.getLogger(SymbolTransformPipeline.class);

	/**
	 * Options supported by {@link SymbolTransformPipeline}.
	 * <p>Custom representation of options required by this particular pipeline class.
	 * Any property not covered by the base PipelineOptions class should be added to a subclass.
	 * This will take advantage of the mapping of any CLI parameters to Java class properties.</p>
	 */
	public static interface SymbolTransformOptions extends PipelineOptions {

		@Description("Path to input file")@Default.String("")
		String getInputFilePath();
		void setInputFilePath(String value);

		@Description("Output file path")@Default.String("")
		String getOutputFilePath();
		void setOutputFilePath(String value);

		@Description("Output table")@Default.String("")
		String getOutputTable();
		void setOutputTable(String value);
	}

	/**
	 * {@link Pipeline} Entry point of this Dataflow Pipeline
	 */
	public static void main(String args[]) {

		try {

			ExtractContractTerms fn = new ExtractContractTerms();

			// fromArgs takes command-line options and maps them to properties within a PipelineOptions 
			SymbolTransformOptions options = PipelineOptionsFactory.fromArgs(args).as(SymbolTransformOptions.class);
			Pipeline pipeline = Pipeline.create(options);

			// Input is a PCollection of String, Output is a PCollection of OptionTick
			PCollection < OptionsTick > mainCollection = pipeline.apply(TextIO.Read.named("Reading input file")
				.from(options.getInputFilePath()))
				.apply(ParDo.named("Extracting options contract terms from symbol")
				.of(fn)).setCoder(SerializableCoder.of(OptionsTick.class));

			// Input is PCollection of OptionTick, Output are records within the BigQuery
			// If destination is BigQuery then DataflowPipelineRunner or BlockingDataflowPipelineRunner must be used
			if (!"".equals(options.getOutputTable())) {

				mainCollection.apply(ParDo.named("Creating BigQuery row from tick")
					.of(new CreateBigQueryRow()))
					.apply(BigQueryIO.Write.named("Writing records to BigQuery")
					.to(options.getOutputTable())
					.withSchema(SymbolTransformPipeline.generateSchema())
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

			} else {

				// To write to a local text file, please verify that 
				// DirectPipelineRunner is selected in pom.xml or the run shell script
				mainCollection.apply(ParDo.named("Creating String row from tick")
					.of(new TickToString()))
					.apply(TextIO.Write.named("Writing output file")
					.to(options.getOutputFilePath()));
			}

			pipeline.run();
			System.exit(0);

		} catch (Exception ex) {
			LOGGER.error(ex.getMessage(), ex);
			System.exit(1);
		}
	}

	public static TableSchema generateSchema() {
		List < TableFieldSchema > fields = new ArrayList < TableFieldSchema > ();

		fields.add(new TableFieldSchema().setName("EXCHANGE_TIMESTAMP").setType("TIMESTAMP"));
		fields.add(new TableFieldSchema().setName("INSERT_TIMESTAMP").setType("TIMESTAMP"));
		fields.add(new TableFieldSchema().setName("UNDERLYING").setType("STRING"));
		fields.add(new TableFieldSchema().setName("EXPIRATION_YEAR").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("EXPIRATION_MONTH").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("EXPIRATION_DAY").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("PUT_CALL").setType("STRING"));
		fields.add(new TableFieldSchema().setName("STRIKE_PRICE").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("BID_SIZE").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("BID").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("ASK").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("ASK_SIZE").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("TRADE").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("TRADE_SIZE").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("EXCHANGE").setType("STRING"));

		return new TableSchema().setFields(fields);
	}
}
