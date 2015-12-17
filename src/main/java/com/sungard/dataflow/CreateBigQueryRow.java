package com.sungard.dataflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;

/**
 * {@link DoFn} derivative (no pun intended) class containing the principal logic for transforming options market data into BigQuery
 * <p>
 * Class definition extends DoFn, which is represented to take in a String and return a String.
 * Strings are obviously unremarkable, but the API supports using more exotic classes of PTransforms as well.
 */
public class CreateBigQueryRow extends DoFn < OptionsTick, TableRow > {
	private static final long serialVersionUID = 7935252322013725933L;
	private static final Logger LOGGER = LoggerFactory.getLogger(ExtractContractTerms.class);
	private final Aggregator < Integer, Integer > processed;
	private final Aggregator < Integer, Integer > exceptions;

	public static final TableRow toBigQuery(OptionsTick tick) {
		TableRow row = new TableRow();

		row.set("EXCHANGE_TIMESTAMP", tick.exchangeTimestamp);
		row.set("INSERT_TIMESTAMP", tick.insertTimestamp);
		row.set("UNDERLYING", tick.symbol.underlying);
		row.set("EXPIRATION_YEAR", tick.symbol.expirationYear); // Y2.1K bug
		row.set("EXPIRATION_MONTH", tick.symbol.expirationMonth);
		row.set("EXPIRATION_DAY", tick.symbol.expirationDay);
		row.set("PUT_CALL", tick.symbol.putCall);
		row.set("STRIKE_PRICE", tick.symbol.strikePrice.floatValue());
		row.set("BID_SIZE", tick.bidSize);
		row.set("BID", tick.bid);
		row.set("ASK", tick.ask);
		row.set("ASK_SIZE", tick.askSize);
		row.set("TRADE", tick.trade);
		row.set("TRADE_SIZE", tick.tradeSize);

		return row;
	}

    public CreateBigQueryRow() {
        this.processed = createAggregator("Processed symbols", new Sum.SumIntegerFn());
        this.exceptions = createAggregator("Exceptions", new Sum.SumIntegerFn());
    }

	/**
	 * Take an options symbol tick and return a BigQuery TableRow
	 */
	public void processElement(ProcessContext c) {
		try {
			c.output(toBigQuery(c.element()));
			processed.addValue(1); // increment aggregator
			return;

		} catch (Exception ex) {
			exceptions.addValue(1);
			LOGGER.warn(ex.getMessage(), ex);
		}
	}
}
