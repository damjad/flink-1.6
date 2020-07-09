package org.apache.flink.morpheus.generators;

import org.apache.flink.morpheus.generators.utils.WordUtils;
import org.apache.flink.morpheus.generators.utils.vo.CustomString;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractSentenceGenerator implements RandomSentenceGenerator {

	protected long duration;
	protected int rate;
	protected transient List<CustomString> wordList;
	protected int wordLength = 4;

	public AbstractSentenceGenerator(int wordLength, long duration, int rate) {
		this();
		this.wordLength = wordLength;
		this.duration = duration;
		this.rate = rate;
	}

	public AbstractSentenceGenerator() {

	}

	@Override
	public List<CustomString> nextSentence(int desiredSentenceSize) {
		if (wordList == null) {
			prepareWordList();
		}

		List<CustomString> builder = new ArrayList<>();
		while (desiredSentenceSize > 0) {
			CustomString word = CustomString.copyOf(nextWord());
			word.setEventTime(System.currentTimeMillis());
			desiredSentenceSize--;
			builder.add(word);
		}
		return builder;
	}

	private synchronized void prepareWordList() {
		this.wordList = WordUtils.genCustomStringWords(wordLength);
	}

	@Override
	public long getDuration() {
		return duration;
	}

	@Override
	public int getRate() {
		return rate;
	}

	public abstract CustomString nextWord();
}
