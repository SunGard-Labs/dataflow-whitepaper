package com.sungard.dataflow;

import java.util.ArrayList;
import java.util.List;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * This {@code DoFn} is used to create batches of elements to be processed as a whole further down
 * the pipeline.
 * <p><b>Implementation note:</b> Lists of {@link ElementCollectorOptions#getBatchSize()}-length are created
 * by the elements that flow into this {@code DoFn}. The last batch might be smaller as there might be not
 * enough elements in the input PCollection.</p>
 * <p><b>Usage:</b><pre>
	PCollection&lt;String> stringValues = ...
	ElementCollectorOptions options = ...

	PCollection&lt;List&lt;String>> listOfStringValues = stringValues.apply(
		ParDo.named("Creating Batches of " + options.getBatchSize())
			.of(new ElementCollector&lt;String>(options)));
 * </pre></p>
 *
 * @param <T> The type of elements to gather
 */
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

	public static void main(String... args) {
	    PCollection<String> stringValues = null;
		ElementCollectorOptions options = null;

		PCollection<List<String>> listOfStringValues = stringValues.apply(
				ParDo.named("Creating Batches of " + options.getBatchSize())
					.of(new ElementCollector<String>(options)));
		
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
