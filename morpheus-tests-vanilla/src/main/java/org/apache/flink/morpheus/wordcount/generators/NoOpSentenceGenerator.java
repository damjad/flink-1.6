package org.apache.flink.morpheus.wordcount.generators;

import org.apache.flink.morpheus.wordcount.generators.utils.vo.CustomString;

import java.util.List;

public class NoOpSentenceGenerator implements RandomSentenceGenerator {

	@Override
	public List<CustomString> nextSentence(int desiredSentenceSize) {
		return null;
	}

	@Override
	public int getRate() {
		return 1;
	}

	@Override
	public long getDuration() {
		return 0;
	}
}
