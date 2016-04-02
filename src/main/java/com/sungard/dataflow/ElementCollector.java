package com.sungard.dataflow;

import java.util.ArrayList;
import java.util.List;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Sum;

public class ElementCollector<T> extends DoFn<T, List<T>> {
	private static final long serialVersionUID = 1L;

	public static interface ElementCollectorOptions {
		int getBatchSize();
		void setBatchSize(int batchSize);
	}

	private final Aggregator<Integer, Integer> collectedElements;
	private final Aggregator<Integer, Integer> flushes;

	private final int batchSize;
	private ArrayList<T> buffer;

	public ElementCollector(ElementCollectorOptions options) {
		this.batchSize = options.getBatchSize();

		this.collectedElements = createAggregator("Collected elements", new Sum.SumIntegerFn());
		this.flushes = createAggregator("Buffer flushes", new Sum.SumIntegerFn());
	}

	@Override
	public void startBundle(Context context) {
		buffer = new ArrayList<>(batchSize);
	}

	@Override
	public void processElement(ProcessContext context) {
		collectedElements.addValue(1);
		buffer.add(context.element());

		if (buffer.size() >= batchSize) {
			flushBuffer(context);
		}
	}

	@Override
	public void finishBundle(Context context) {
		flushBuffer(context);
	}

	private void flushBuffer(Context context) {
		flushes.addValue(1);
		context.output(buffer);
		buffer.clear();
	}
}
