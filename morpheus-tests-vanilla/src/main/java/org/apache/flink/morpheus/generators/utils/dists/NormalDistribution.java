package org.apache.flink.morpheus.generators.utils.dists;

import org.apache.flink.morpheus.generators.utils.RandUtils;

public class NormalDistribution implements RandomDistribution {

	int bound;
	int bias;
	double skew;

	public NormalDistribution(int bound, int bias, double skew) {
		this.bound = bound;
		this.bias = bias;
		this.skew = skew;
	}

	@Override
	public int getBound() {
		return bound;
	}

	@Override
	public void setBound(int bound) {
		this.bound = bound;
	}

	public int getBias() {
		return bias;
	}

	public void setBias(int bias) {
		this.bias = bias;
	}

	public double getSkew() {
		return skew;
	}

	public void setSkew(double skew) {
		this.skew = skew;
	}

	@Override
	public int generateNext() {
		return RandUtils.nextSkewedNormalInt(0, bound, skew, bias);
	}
}
