package com.sungard.dataflow;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Scanner;
import java.util.List;
import java.util.ArrayList;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * Small helper to create a HBase table with given splits:
 * <p>Running this program asks the user (via STDIN) for a table name and column families which should be
 * used in the {@code CREATE} statement. </br>
 * Finally, the amount of splits must be provided by the user. Based on the desired split count,
 * the {@code CREATE} statement is enriched with split boundaries <b>based on the assumption that
 * UUIDs are used as row keys</b> in the HBase table.</p>
 */
public class BigtableSplitGenerator {

	public static String getStatement(int desiredSplitCount) {
		if (desiredSplitCount <= 1) {
			throw new IllegalArgumentException("At least 2 splits must be created!");
		}
		BigDecimal splitCount = BigDecimal.valueOf(desiredSplitCount);
		MathContext mc = new MathContext(100, RoundingMode.UP);
		String maxNumberAsHexString = "ffffffffffffffffffffffffffffffff";
		
		BigDecimal maxNumber = new BigDecimal(new BigInteger(maxNumberAsHexString, 16));
		final BigDecimal divisorConstant = maxNumber.divide(splitCount, mc);
		BigDecimal divisor = divisorConstant;

		List<String> splits = new ArrayList<>();
		while (divisor.compareTo(maxNumber) == -1) {
			splits.add(divisor.toBigInteger().toString(16));
			divisor = divisor.add(divisorConstant);
		}
		
		StringBuilder retValue = new StringBuilder();
		for (String split : splits) {
			StringBuilder s = new StringBuilder(split);
			while (s.length() < 32) {
				s.insert(0, "0");
			}

			// Format as UUID again
			retValue.append("'")
					.append(s.substring(0, 8)).append("-")
					.append(s.substring(8, 12)).append("-")
					.append(s.substring(12, 16)).append("-")
					.append(s.substring(16, 20)).append("-")
					.append(s.substring(20, 32)).append("',");
		}
		retValue.deleteCharAt(retValue.length() - 1);
		return retValue.toString();
	}

	public static void main(String[] args) {
		try (Scanner input = new Scanner(System.in)) {
			System.out.print("Enter table name: ");
			String tableName = input.nextLine();

			System.out.print("Enter data column families. ex. \"{NAME =>'"
					+ "CF1'},{NAME => 'CF2'}\": ");
			String families = input.nextLine();

			System.out.print("Enter number of desired splits (should be x12 the number of nodes in the Bigtable cluster): ");
			int numCluster = input.nextInt();

			String str = getStatement(numCluster);

			System.out.println("\nHbase shell command to copy paste:\n");
			System.out.println("CREATE " + "'" + tableName 
					+ "'," + families
					+ ", SPLITS => [" + str + "]");
		}
	}
}