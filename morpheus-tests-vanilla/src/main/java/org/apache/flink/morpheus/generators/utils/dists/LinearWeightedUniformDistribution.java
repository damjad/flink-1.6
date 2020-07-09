package org.apache.flink.morpheus.generators.utils.dists;

import java.io.Serializable;
import java.util.function.UnaryOperator;

public class LinearWeightedUniformDistribution extends WeightedDistribution {

	public LinearWeightedUniformDistribution(int slope, int constant, int maxVal) {
		super(new LineFunction(slope, constant), new UniformDistribution(maxVal));
	}
}

class LineFunction implements Serializable, UnaryOperator<Integer> {

	private int slope;
	private int constant;

	public LineFunction(int slope, int constant) {
		this.slope = slope;
		this.constant = constant;
	}

	@Override
	public Integer apply(Integer integer) {
		return slope * integer + constant;
	}
}
