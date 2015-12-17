package com.sungard.dataflow;

import static org.junit.Assert.assertNotNull;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.transforms.DoFnTester;

public class ExtractContractTermsTest {

    private DoFnTester < String, OptionsTick > underTest;

    @Before
    public void setUp() {
        underTest = DoFnTester.of(new ExtractContractTerms());
    }
    
    @Test
    public void executeSampleRun() throws IOException {
        
        try (LineNumberReader lnis = new LineNumberReader(new InputStreamReader(
                new BufferedInputStream(new FileInputStream("src/test/resources/input.txt"))))) {
            
            String line;
            while ((line = lnis.readLine()) != null) {
           	   	List<OptionsTick> actual = underTest.processBatch(line);
				assertNotNull((Object) actual);
			 }
        }
    }
}
