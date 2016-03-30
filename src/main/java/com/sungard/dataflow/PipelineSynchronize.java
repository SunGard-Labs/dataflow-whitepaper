package com.sungard.dataflow;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

/**
 * Uses a {@link GroupByKey} to join the pipeline, so that all workers will have submitted
 * all work units before the following {@code ParDo} is executed.
 * <p><b>Usage:</b> There are still (empty) files created by the workers thus there is
 * some overhead for the shuffling itself. Therefore use only if you have to.
 * <pre>
	PCollection<String> firstPartValues = ...;
	
	PCollectionView<?> waitForProcessingCompleted = firstPartValues.apply(new PipelineSynchronize<String>());
	
	PCollection<Long> secondPartValues = ...;
	secondPartValues.apply(ParDo.named("Processing something after the first part finished").of(new DoFn<Long, String>(){

		public void processElement(ProcessContext c) {
			// Whatever happened in firstPartValues is processed
			// This is also blocking upstream: as long as the side input waitForProcessingCompleted 
			// isn't there, also secondPartValues is not yet evaluated. And waitForProcessingCompleted
			// will only be available if each element from firstPartValues has been completely processed
		}}).withSideInputs(waitForProcessingCompleted));
   </pre>
 * </p>
 * <p><b>Implementation note:</b> Works by consuming each incoming element and then doing a 
 * GroupBy on this {@link KV<Void, Void>} which is an empty result.<br>
 * But as this is only used as a 'marker view' nobody cares for the content anyway.</p>
 */
public class PipelineSynchronize<T> extends PTransform<PCollection<T>, PCollectionView<?>> {
	private static final long serialVersionUID = 1L;

	private static class Blackhole<T> extends DoFn<T, KV<Void, Void>> {
		private static final long serialVersionUID = 1L;

        @Override
        public void processElement(ProcessContext c) throws Exception {
            //NoOp - whatever the input is: Just consume it
        }
    }

	@Override
	public String getName() {
		return "Synchronize all workers";
	}

    @Override
    public PCollectionView<?> apply(PCollection<T> input) {
    	return input.apply(ParDo.of(new Blackhole<T>()))
            // Use a GroupBy on PCollection<Void> creates an empty view -
    		// but nobody cares for the content anyway
            .apply(GroupByKey.<Void, Void>create())
            .apply(View.<KV<Void, Iterable<Void>>>asSingleton());
    }
}
