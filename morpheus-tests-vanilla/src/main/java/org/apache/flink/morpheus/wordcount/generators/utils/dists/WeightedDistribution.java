package org.apache.flink.morpheus.wordcount.generators.utils.dists;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WeightedDistribution implements RandomDistribution {

	UnaryOperator<Integer> shapeFunction;
	RandomDistribution distribution;

	/**
	 * List containing elements repeated f(x) times.
	 */
	List<Integer> distList;
	int[] distArr;

	public WeightedDistribution(UnaryOperator<Integer> shapeFunction, RandomDistribution distribution) {
		this.shapeFunction = shapeFunction;
		this.distribution = distribution;
	}

	@Override
	public int getBound() {
		return distribution.getBound();
	}

	@Override
	public void setBound(int bound) {
		if (distribution.getBound() != bound) {
			distList = null;
			distribution.setBound(bound);
		}
	}

	@Override
	public int generateNext() {
		if (distList == null) {
			distList = new ArrayList<>();

			distList.addAll(IntStream.range(0, distribution.getBound()).boxed().
				flatMap(x -> IntStream.generate(() -> x).boxed().limit(shapeFunction.apply(x))).
				collect(Collectors.toList()));

			distArr = IntStream.range(0, distribution.getBound()).boxed().
				flatMap(x -> IntStream.generate(() -> x).boxed().limit(shapeFunction.apply(x)))
				.mapToInt(x-> x).toArray();

			distribution.setBound(distList.size());
		}

		return distArr[distribution.generateNext()];
	}
}
