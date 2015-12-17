package com.sungard.dataflow;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class OptionsSymbolTest {

    private OptionsSymbol underTest;

    @Before
    public void setUp() throws Exception {
        underTest = new OptionsSymbol("ZVZZT151218P00009000");
    }
    
    @Test
    public void testToString() {
        assertEquals("ZVZZT\t15\t12\t18\tP\t9\t", underTest.toString());
    }
}
