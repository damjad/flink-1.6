package org.apache.flink.morpheus.sources;

import org.apache.flink.morpheus.generators.RandomSentenceGenerator;
import org.apache.flink.morpheus.generators.utils.vo.CustomString;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;

import java.util.Iterator;
import java.util.List;

public class MultiDistRateControlledSourceFunction extends RichParallelSourceFunction<List<CustomString>> {

	/**
	 * the length of each sentence (in chars)
	 **/
	private final int sentenceSize;

	/**
	 * The generators with duration. The duration is in seconds (> 2). Source will iterate through the generators for a particular duration of time.
	 */
	private final List<RandomSentenceGenerator> generators;

	private transient RateLimiter rateLimiter;

	private volatile boolean running = true;

	public MultiDistRateControlledSourceFunction(int size, List<RandomSentenceGenerator> generators) {
		this.generators = generators;
		this.sentenceSize = size;
	}

	@Override
	public void run(SourceContext<List<CustomString>> ctx) throws Exception {

		Iterator<RandomSentenceGenerator> it = generators.iterator();
		Long lastStartTime = System.currentTimeMillis();

		RandomSentenceGenerator gen = it.next();
		rateLimiter = RateLimiter.create(gen.getRate());

		while (running) {
			long emitStartTime = System.currentTimeMillis();
			if (emitStartTime > lastStartTime + gen.getDuration() && it.hasNext()) {
				lastStartTime = emitStartTime;
				gen = it.next();
				rateLimiter.setRate(gen.getRate());
			}

			rateLimiter.acquire();

			List<CustomString> sentence = gen.nextSentence(sentenceSize);
			if (null != sentence && !sentence.isEmpty()) {
				ctx.collect(sentence);
			}
		}

		ctx.close();
	}

	@Override
	public void cancel() {
		running = false;
	}
}

