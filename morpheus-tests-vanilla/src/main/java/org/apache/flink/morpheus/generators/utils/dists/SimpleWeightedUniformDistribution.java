package org.apache.flink.morpheus.generators.utils.dists;

import java.io.Serializable;
import java.util.function.UnaryOperator;

public class SimpleWeightedUniformDistribution extends WeightedDistribution {
	public SimpleWeightedUniformDistribution(int bound, int... weights) {
		super(new WeightFunction(weights), new UniformDistribution(bound));
	}
}
class WeightFunction implements UnaryOperator<Integer>, Serializable {

	int[] weights;
	public WeightFunction(int ... weights) {
		this.weights = weights;
	}

	@Override
	public Integer apply(Integer i) {
		return weights[i];
	}
}
