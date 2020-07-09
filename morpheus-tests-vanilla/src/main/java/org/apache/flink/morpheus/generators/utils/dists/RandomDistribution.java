package org.apache.flink.morpheus.generators.utils.dists;

import java.io.Serializable;

public interface RandomDistribution extends Serializable {
	public static enum Type {
		NORMAL,
		UNIFORM,
		LINEAR_WEIGHTED,
		SIMPLE_WEIGHTED,
		MASKED,
		ROUND_ROBIN
	}
	int getBound();
	void setBound(int bound);
	int generateNext();
}
