package org.apache.flink.morpheus.wordcount.generators.utils;

import java.util.Random;

public class RandUtils {

	private static final Random rand = new Random();

	public static int nextSkewedNormalInt(int min, int max, double skew, int bias) {
		double range = (double) (max - min);
		double mid = min + range / 2.0;
		double unitGaussian = rand.nextGaussian();
		double biasFactor = Math.exp(bias);
		double retval = mid + (range * (biasFactor / (biasFactor + Math.exp(-unitGaussian / skew)) - 0.5));
		return (int) Math.min(max, Math.max(retval, min));
	}

	public static int nexUniformInt(int bound) {
		return rand.nextInt(bound);
	}


}
