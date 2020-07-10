package org.apache.flink.morpheus.wordcount.generators;

import org.apache.flink.morpheus.wordcount.generators.utils.vo.CustomString;

import java.io.Serializable;
import java.util.List;

public interface RandomSentenceGenerator extends Serializable {
	enum GenType {
		UNIFORM,
		NOOP,
		NORMAL,
		CUSTOM;
	}

	List<CustomString> nextSentence(int desiredSentenceSize);
	int getRate();
	long getDuration();
}
