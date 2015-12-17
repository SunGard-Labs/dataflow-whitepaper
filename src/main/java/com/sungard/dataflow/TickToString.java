package com.sungard.dataflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;

/**
 * {@link DoFn} derivative (no pun intended) class containing the principal logic for transforming options market data into a tab-separated value (TSV)
 * <p>
 * Class definition extends DoFn, which is represented to take in a String and return a String.
 * Strings are obviously unremarkable, but the API supports using more exotic classes of PTransforms as well.
 */
public class TickToString extends DoFn < OptionsTick, String > {
	private static final long serialVersionUID = 7935252322013725933L;
	private static final Logger LOGGER = LoggerFactory.getLogger(ExtractContractTerms.class);
	private final Aggregator < Integer, Integer > processed;
	private final Aggregator < Integer, Integer > exceptions;

	public TickToString() {
		this.processed = createAggregator("Processed symbols", new Sum.SumIntegerFn());
		this.exceptions = createAggregator("Exceptions", new Sum.SumIntegerFn());
	}

	/**
	 * Take an options symbol tick and return a tab-separated value
	 * <p>
	 */
	public void processElement(ProcessContext c) {
		try {
			c.output(c.element().toString());
			processed.addValue(1); // increment aggregator
			return;
		} catch (Exception ex) {
			exceptions.addValue(1);
			LOGGER.warn(ex.getMessage(), ex);
		}
	}
}
