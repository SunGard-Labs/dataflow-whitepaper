package com.sungard.dataflow;

import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PInput;

/**
 * This class can be used to wait for a single PubSub message to start a pipeline with a delay
 * or wait in the middle of a pipline for a single event - Therefore it is wrapped as a {@link 
 * PCollectionView} to use as side input right away.
 * </p><b>Usage:</b>
 * <pre>
    PCollection<String> values = ...
    
    values.apply(ParDo.named("Processing").of(new DoFn<String, String>(){
    
    	public void processElement(ProcessContext c) {
    		// Run this DoFn as soon as the first elements are in values
    		// AND a single PubSub was received
    	}}).withSideInputs(p.apply(new PubSubStarter(null))));
 * </pre></p>
 */
public class PubSubStarter extends PTransform<PInput, PCollectionView<String>>{
	private static final long serialVersionUID = 1L;
    private final String topic;

    public static interface PubSubDelayOptions extends DataflowPipelineOptions { 
    	String getTopic();
    	void setTopic(String s);
    }

    public PubSubStarter(PubSubDelayOptions options){
        topic = "projects/" + options.getProject() 
        	+ "/topics/" + options.getTopic();
    }
    
    @Override
    public String getName() {
    	return "Wait for PubSub via " + topic;
    }

    @Override
    public PCollectionView<String> apply(PInput input) {
		return input.getPipeline()
    		.apply(PubsubIO.Read.topic(topic).maxNumRecords(1))
            .apply(View.<String>asSingleton());
    }
}
