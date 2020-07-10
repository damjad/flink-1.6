package org.apache.flink.morpheus.wordcount.generators.utils.dists;

public class RoundRobinDistribution implements RandomDistribution {

	int bound;
	int previous;

	public RoundRobinDistribution(int bound) {
		this.bound = bound;
	}

	@Override
	public int getBound() {
		return bound;
	}

	@Override
	public void setBound(int bound) {
		this.bound = bound;
	}

	public int getPrevious() {
		return previous;
	}

	public void setPrevious(int previous) {
		this.previous = previous;
	}

	@Override
	public int generateNext() {
		if (previous >= bound) {
			previous = 0;
		}
		return previous++;
	}
}
