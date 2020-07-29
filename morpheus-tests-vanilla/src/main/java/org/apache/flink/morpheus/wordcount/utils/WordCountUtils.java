package org.apache.flink.morpheus.wordcount.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.morpheus.wordcount.generators.utils.vo.CustomString;
import org.apache.flink.morpheus.wordcount.generators.utils.vo.RecordWrapper;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;

public class WordCountUtils {
	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1L));
				}
			}
		}
	}

	public static final class CountWords extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

		private transient ReducingState<Long> count;

		@Override
		public void open(Configuration parameters) throws Exception {

			ReducingStateDescriptor<Long> descriptor =
				new ReducingStateDescriptor<Long>(
					"count", // the state name
					new Count(),
					BasicTypeInfo.LONG_TYPE_INFO);

			count = getRuntimeContext().getReducingState(descriptor);
		}

		@Override
		public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {
			count.add(value.f1);
			out.collect(new Tuple2<>(value.f0, count.get()));
		}

		public static final class Count implements ReduceFunction<Long> {

			@Override
			public Long reduce(Long value1, Long value2) throws Exception {
				return value1 + value2;
			}
		}
	}


	public static class WordCountWindowAccumulator implements Serializable {
		private CustomString word;
		private long lastIngestionTimestamp = 0;
		private long lastTimestamp = 0;
		private long count = 1;

		public WordCountWindowAccumulator(CustomString word) {
			this.word = word;
		}

		public WordCountWindowAccumulator(CustomString word, long lastIngestionTimestamp, long lastTimestamp, long count) {
			this.word = word;
			this.lastIngestionTimestamp = lastIngestionTimestamp;
			this.lastTimestamp = lastTimestamp;
			this.count = count;
		}

		public WordCountWindowAccumulator add(RecordWrapper<Tuple2<CustomString, Long>> e) {

			if (lastTimestamp < e.getEventTime()) {
				lastTimestamp = e.getEventTime();
				lastIngestionTimestamp = e.getIngestionTime();
			}
			count += e.getRecord().f1;
			return this;
		}

		public RecordWrapper<Tuple2<CustomString, Long>> toOutput() {
			RecordWrapper<Tuple2<CustomString, Long>> r = new RecordWrapper<>(new Tuple2<>(word, count), lastIngestionTimestamp, lastTimestamp);
			r.setKey(word);
			return r;
		}

		public WordCountWindowAccumulator merge(WordCountWindowAccumulator that) {
			count += that.count;
			lastTimestamp = Math.max(lastTimestamp, that.lastTimestamp);
			lastIngestionTimestamp = Math.max(lastIngestionTimestamp, that.lastIngestionTimestamp);
			return this;
		}
	}

	public static class WordCountAggregator extends KeyedProcessFunction<CustomString, RecordWrapper<Tuple2<CustomString, Long>>, RecordWrapper<Tuple2<CustomString, Long>>> implements CheckpointedFunction {

		private final long windowSize;

		private transient long seenSoFar;

		private transient HashMap<CustomString, WordCountWindowAccumulator> temp;
		private transient MapState<CustomString, WordCountWindowAccumulator> state;

		public WordCountAggregator(long windowSize) {
			this.windowSize = windowSize;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			state.clear();
			for (Map.Entry<CustomString, WordCountWindowAccumulator> e : temp.entrySet()) {
				state.put(e.getKey(), e.getValue());
			}
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getKeyedStateStore().getMapState(new MapStateDescriptor<>("state", TypeInformation.of(CustomString .class), TypeInformation.of(WordCountWindowAccumulator.class)));
			temp = new HashMap<>((int) Math.pow(26, 5));
		}


		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			seenSoFar = 0;
		}

		@Override
		public void processElement(RecordWrapper<Tuple2<CustomString, Long>> value, Context ctx, Collector<RecordWrapper<Tuple2<CustomString, Long>>> out) throws Exception {

			Thread.sleep(ThreadLocalRandom.current().nextInt(200,300));
			WordCountWindowAccumulator old = temp.compute(value.getRecord().f0, new BiFunction<CustomString, WordCountWindowAccumulator, WordCountWindowAccumulator>() {
				@Override
				public WordCountWindowAccumulator apply(CustomString key, WordCountWindowAccumulator acc) {
					if(acc == null) {
						acc = new WordCountWindowAccumulator(key);
						acc.add(value);
					}
					else {
						acc.add(value);
					}

					return acc;
				}
			});
			if (old == null || old.count == 1) {
				ctx.timerService().registerEventTimeTimer(windowSize); // [BUG] - Timer cannot be registered.
			} else if (seenSoFar++ % windowSize == 0) {
				out.collect(old.toOutput());
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<RecordWrapper<Tuple2<CustomString, Long>>> out) throws Exception {
			super.onTimer(timestamp, ctx, out);
			WordCountWindowAccumulator acc;
			if ((acc = temp.remove(ctx.getCurrentKey())) != null) {
				out.collect(acc.toOutput());
			}
		}
	}

	public static final class CountWordsCustomString extends RichFlatMapFunction<Tuple2<CustomString, Long>, Tuple2<CustomString, Long>> {

		private transient ReducingState<Long> count;
		private transient Counter counter;

		@Override
		public void open(Configuration parameters) throws Exception {

			ReducingStateDescriptor<Long> descriptor =
				new ReducingStateDescriptor<Long>(
					"count", // the state name
					new CountWordsCustomString.Count(),
					BasicTypeInfo.LONG_TYPE_INFO);

			count = getRuntimeContext().getReducingState(descriptor);
			this.counter = getRuntimeContext()
				.getMetricGroup()
				.counter("myCounter");
		}

		@Override
		public void flatMap(Tuple2<CustomString, Long> value, Collector<Tuple2<CustomString, Long>> out) throws Exception {
			this.counter.inc(value.f1);
			count.add(value.f1);
			out.collect(new Tuple2<>(value.f0, count.get()));
		}

		public static final class Count implements ReduceFunction<Long> {

			@Override
			public Long reduce(Long value1, Long value2) throws Exception {
				return value1 + value2;
			}
		}
	}


	public static final class CustomStringTokenizer implements FlatMapFunction<List<CustomString>, RecordWrapper<Tuple2<CustomString, Long>>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(List<CustomString> value, Collector<RecordWrapper<Tuple2<CustomString, Long>>> out) throws Exception {
			value.forEach(x -> {
				RecordWrapper<Tuple2<CustomString, Long>> r = new RecordWrapper<>(new Tuple2<>(x, 1L), System.currentTimeMillis());
				r.setEventTime(x.getEventTime());
				out.collect(r);
			});
		}
	}


	public static final class CustomStringTokenizer2 implements FlatMapFunction<CustomString[], RecordWrapper<Tuple2<CustomString, Long>>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(CustomString[] value, Collector<RecordWrapper<Tuple2<CustomString, Long>>> out) throws Exception {
			Arrays.stream(value).forEach(x -> {
				RecordWrapper<Tuple2<CustomString, Long>> r = new RecordWrapper<>(new Tuple2<>(x, 1L), System.currentTimeMillis());
				r.setEventTime(x.getEventTime());
				out.collect(r);
			});
		}
	}

}
