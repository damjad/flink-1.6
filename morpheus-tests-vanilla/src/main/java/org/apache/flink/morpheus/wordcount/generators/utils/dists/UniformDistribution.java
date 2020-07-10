package org.apache.flink.morpheus.wordcount.generators.utils.dists;

import org.apache.flink.morpheus.wordcount.generators.utils.RandUtils;

public class UniformDistribution implements RandomDistribution {

	private int bound;

	public UniformDistribution(){

	}

	public UniformDistribution(int bound) {
		this.bound = bound;
	}

	@Override
	public int getBound() {
		return this.bound;
	}

	@Override
	public void setBound(int bound) {
		this.bound = bound;
	}

	@Override
	public int generateNext() {
		return RandUtils.nexUniformInt(getBound());
	}
}
