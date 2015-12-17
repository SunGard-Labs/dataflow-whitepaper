package com.sungard.dataflow;

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;

/**
 * {@link DoFn} derivative (no pun intended) class containing the principal logic for transforming options market data into BigQuery
 * <p>
 * Class definition extends DoFn, which is represented to take in a String and return a String.
 * Strings are obviously unremarkable, but the API supports using more exotic classes of PTransforms as well.
 */
public class ExtractContractTerms extends DoFn < String, OptionsTick > {
	private static final long serialVersionUID = 7935252322013725933L;
	private static final Logger LOGGER = LoggerFactory.getLogger(ExtractContractTerms.class);
	private final Aggregator < Integer, Integer > processed;
	private final Aggregator < Integer, Integer > exceptions;

	public ExtractContractTerms() {
		this.processed = createAggregator("Processed symbols", new Sum.SumIntegerFn());
		this.exceptions = createAggregator("Exceptions", new Sum.SumIntegerFn());
	}

	/**
	 * Extract the contract terms from a single market data record's option symbol field, which follows standard OCC symbology.
	 * <p>
	 */
	public void processElement(ProcessContext c) {
		try {

			String line = c.element();
			String[] values = line.split("\t");
			String sym = values[0];
			OptionsTick tick = new OptionsTick();

			tick.exchangeTimestamp = ("".equals(values[1]) ? null : Long.parseLong(values[1]));
			tick.symbol = new OptionsSymbol(sym);
			tick.exchangeTimestamp = new Long(values[1]).longValue();

			if (values[2] != null) {
				if (values[2].length() > 0) {
					tick.bidSize = new Integer(values[2]);
				}
			}
			if (values[3] != null) {
				if (values[3].length() > 0) {
					tick.bid = new BigDecimal(values[3]);
				}
			}
			if (values[4] != null) {
				if (values[4].length() > 0) {
					tick.ask = new BigDecimal(values[4]);
				}
			}
			if (values[5] != null) {
				if (values[5].length() > 0) {
					tick.askSize = new Integer(values[5]);
				}
			}
			if (values[6] != null) {
				if (values[6].length() > 0) {
					tick.trade = new BigDecimal(values[6]);
				}
			}
			if (values[7] != null) {
				if (values[7].length() > 0) {
					tick.tradeSize = new Integer(values[7]);
				}
			}

			tick.exchange = values[8];
			tick.insertTimestamp = System.currentTimeMillis() / 1000L;

			c.output(tick); // adds to the output PCollection
			processed.addValue(1); // increment aggregator
			return;

		} catch (Exception ex) {
			// Aggregate exception counts for at-a-glance monitoring via Dataflow console
			exceptions.addValue(1);
			LOGGER.warn(ex.getMessage(), ex);
			return;
		}
	}
}
