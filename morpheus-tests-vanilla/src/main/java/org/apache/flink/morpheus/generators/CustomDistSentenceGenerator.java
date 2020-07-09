package org.apache.flink.morpheus.generators;

import org.apache.flink.morpheus.generators.utils.vo.CustomString;
import org.apache.flink.morpheus.generators.utils.dists.WeightedDistribution;
import org.apache.flink.morpheus.generators.utils.dists.RandomDistribution;
import org.apache.flink.util.Preconditions;

public class CustomDistSentenceGenerator extends AbstractSentenceGenerator {

	private RandomDistribution opDistribution;
	private RandomDistribution vNodeDistribution;
	private RandomDistribution wordDistribution;
	private int parallelism;
	private int maxParallelism;
	private int virtualNodesPerOp;

	public CustomDistSentenceGenerator(long duration, int rate, int wordLength, int maxParallelism, RandomDistribution opDistribution, RandomDistribution vNodeDistribution, RandomDistribution wordDistribution) {
		super(wordLength, duration, rate);
		this.opDistribution = opDistribution;
		this.vNodeDistribution = vNodeDistribution;

		Preconditions.checkArgument(!(this.wordDistribution instanceof WeightedDistribution));
		this.wordDistribution = wordDistribution;

		this.maxParallelism = maxParallelism;

		this.parallelism = opDistribution.getBound();
		this.virtualNodesPerOp = vNodeDistribution.getBound();
	}


	public CustomString nextWord() {
		int randOpId = getNextOpId();
		int randVNodeId = getNextVNodeId(randOpId);
		int randWordId = getNextWordId(randVNodeId);
		return wordList.get(randWordId);
	}

	public int getNextOpId() {
		return opDistribution.generateNext();
	}

	public int getNextVNodeId(int opId) {
		int k = vNodeDistribution.generateNext();
		return k * parallelism + opId;
	}

	private int getNextWordId(int vNodeId) {
		double pV = ((double) parallelism * virtualNodesPerOp);
		int bound = (int) Math.ceil((wordList.size() - maxParallelism * vNodeId / pV) / (double) maxParallelism);
		wordDistribution.setBound(bound);
		int k = wordDistribution.generateNext();
		return (int) Math.ceil(maxParallelism * (k + vNodeId / pV));
	}
}
