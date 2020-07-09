package org.apache.flink.morpheus.sources;

import org.apache.flink.morpheus.generators.UniformSentenceGenerator;
import org.apache.flink.morpheus.generators.utils.vo.CustomString;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;

public class RateControlledSourceFunction extends RichParallelSourceFunction<List<CustomString>> {

    /** how many sentences to output per second **/
    private final int sentenceRate;

    /** the length of each sentence (in chars) **/
    private final int sentenceSize;

    private final UniformSentenceGenerator generator;

    private volatile boolean running = true;

    public RateControlledSourceFunction(int rate, int size) {
        sentenceRate = rate;
        generator = new UniformSentenceGenerator();
        sentenceSize = size;
    }

    @Override
    public void run(SourceContext<List<CustomString>> ctx) throws Exception {

        while (running) {
            long emitStartTime = System.currentTimeMillis();
            for (int i = 0; i < sentenceRate; i++) {
                ctx.collect(generator.nextSentence(sentenceSize));
            }
            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000) {
                Thread.sleep(1000 - emitTime);
            }
        }

        ctx.close();
    }

    @Override
    public void cancel() {
        running = false;
    }
}
