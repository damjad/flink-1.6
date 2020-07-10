package org.apache.flink.morpheus.wordcount.generators;

import org.apache.flink.morpheus.wordcount.generators.utils.vo.CustomString;
import org.apache.flink.morpheus.wordcount.generators.utils.RandUtils;

public class UniformSentenceGenerator extends AbstractSentenceGenerator {


	private double skewProbability;

	public UniformSentenceGenerator() {
		super();
	}

	public UniformSentenceGenerator(int wordLength, long duration, int rate) {
		this(wordLength, duration, rate, 0.0);
		this.wordLength = wordLength;
	}

	public UniformSentenceGenerator(int wordLength, long duration, int rate, double skewProbability) {
		super(wordLength, duration, rate);
		this.wordLength = wordLength;
		this.skewProbability = skewProbability;
	}


	public CustomString nextWord() {
		if (RandUtils.nexUniformInt(100) < skewProbability && skewProbability < 100) {
			return new CustomString("skew");
		}
		return wordList.get(RandUtils.nexUniformInt(wordList.size()));
	}
}
