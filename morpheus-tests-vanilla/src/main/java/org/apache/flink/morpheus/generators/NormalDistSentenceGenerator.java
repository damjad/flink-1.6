package org.apache.flink.morpheus.generators;

import org.apache.flink.morpheus.generators.utils.vo.CustomString;
import org.apache.flink.morpheus.generators.utils.dists.NormalDistribution;

public class NormalDistSentenceGenerator extends AbstractSentenceGenerator {

	NormalDistribution wordDistribution;

	public NormalDistSentenceGenerator(int wordLength, long duration, int rate, int bias, double skew) {
		super(wordLength, duration, rate);
		wordDistribution = new NormalDistribution((int) (Math.pow(26, wordLength) - 1), bias, skew);
	}

	public CustomString nextWord() {
		return wordList.get(wordDistribution.generateNext());
	}
}
