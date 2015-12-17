package com.sungard.dataflow;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;

public class OptionsTick implements Serializable { // Must be serializable to be included within a PCollection
    private static final long serialVersionUID = -4502791666748533137L;
    public OptionsSymbol symbol;
	public long exchangeTimestamp;
	public long insertTimestamp;
	public Integer bidSize;
	public BigDecimal bid;
	public BigDecimal ask;
	public Integer askSize;
	public BigDecimal trade;
	public Integer tradeSize;
	public String exchange;

	public String toString() {
		StringBuilder sb = new StringBuilder();
		try {
			for (Field field: this.getClass().getDeclaredFields()) {
			    if ("serialVersionUID".equals(field.getName())) {
			        continue;
			    }
				if ("symbol".equals(field.getName())) {
					sb.append(field.get(this).toString());
				} else {
					if (null != field.get(this)) {
						sb.append(field.get(this).toString());
					} else {
						sb.append("");
					}
					sb.append("\t");
				}
			}
		} catch (Exception ex) {
			sb.append("<exception reading field: ").append(ex.getMessage()).append(">");
			sb.append("\t");
		}
		return sb.toString();
	}


}
