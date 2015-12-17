package com.sungard.dataflow;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;

public class OptionsSymbol implements Serializable { // Must be serializable to be included within a PCollection
 
    private static final long serialVersionUID = 1534579507165981924L;
    public String underlying;
	public int expirationYear;
	public int expirationMonth;
	public int expirationDay;
	public String putCall;
	public BigDecimal strikePrice;

	public OptionsSymbol(String occSym) throws Exception {

		char[] chars = occSym.toCharArray();

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < chars.length; i++) {
			if (!Character.isDigit(chars[i])) {
				sb.append(chars[i]);
			} else {
				this.underlying = sb.toString();
				String sym = occSym.substring(i, occSym.length()); // ZVZZT
				this.expirationYear = Integer.parseInt(sym.substring(0, 2)); // 16 - YY
				this.expirationMonth = Integer.parseInt(sym.substring(2, 4)); // 11 - MM
				this.expirationDay = Integer.parseInt(sym.substring(4, 6)); // 02 - DD
				this.putCall = sym.substring(6, 7); // P(ut) or C(all)
				BigDecimal dollar = new BigDecimal(sym.substring(7, 12)); // 00016 is $16
				BigDecimal decimal = new BigDecimal(sym.substring(12, 15)).divide(new BigDecimal(100)); // 500 is 50 cents
				this.strikePrice = dollar.add(decimal);
				return;
			}
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		try {
			for (Field field: this.getClass().getDeclaredFields()) {
                if ("serialVersionUID".equals(field.getName())) {
                    continue;
                }
				if (null != field.get(this)) {
					sb.append(field.get(this).toString());
				} else {
					sb.append("");
				}
				sb.append("\t");
			}
		} catch (Exception ex) {
			sb.append("<exception reading field").append(ex.getMessage()).append(">");
			sb.append("\t");
		}
		return sb.toString();
	}
}
