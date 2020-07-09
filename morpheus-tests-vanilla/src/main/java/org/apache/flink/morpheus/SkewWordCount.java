package org.apache.flink.morpheus;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.morpheus.generators.RandomSentenceGenerator;
import org.apache.flink.morpheus.generators.SentenceGeneratorFactory;
import org.apache.flink.morpheus.generators.utils.vo.CustomString;
import org.apache.flink.morpheus.generators.utils.vo.RecordWrapper;
import org.apache.flink.morpheus.sinks.LatencySinkFunction;
import org.apache.flink.morpheus.sources.MultiDistRateControlledSourceFunction;
import org.apache.flink.morpheus.utils.PropertiesHandler;
import org.apache.flink.morpheus.utils.WordCountUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SkewWordCount {

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		PropertiesHandler props = PropertiesHandler.getInstance(params.get("properties-file", "src/main/skew-word-count.properties"));

		params.mergeWith(ParameterTool.fromPropertiesFile(props.getFilePath()));

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		cleanup(props);

		createWordCountJobGraph(env,
			props,
			params.getInt("p1", 1),
			params.getInt("p2", 1),
			params.getInt("p3", props.getInteger("parallelism", 1)),
			params.getInt("p4", props.getInteger("parallelism", 1)));

		env.execute();

	}

	public static void cleanup(PropertiesHandler props) throws IOException {
		// remove metrics manager metrics
		FileUtils.cleanDirectory(new File(props.getProperty("policy.rates.path")));

	}

	public static JobGraph createWordCountJobGraph(StreamExecutionEnvironment env,
												   PropertiesHandler props,
												   int... p) {

		List<RandomSentenceGenerator> generators = createGeneratorsStrategy(props);
		int sentenceSize = props.getInteger("sentence-size", 100);
		int defaultParallelism = props.getInteger("default-parallelism", 1);
		int maxParallelism = props.getInteger("max-parallelism", 4096);
		int numOfVirtualNodes = props.getInteger("n-virtual-nodes", 4);
		int checkpointingInterval = props.getInteger("check-pointing-interval", 20_000);
		boolean cancelState = props.getBoolean("cancel-state", true);
		String latenciesOutputPath = props.getProperty("latencies-path");
		int windowSize = props.getInteger("window-size", 300_000);

		configureEnvironment(env, defaultParallelism, maxParallelism, checkpointingInterval, cancelState);
		// set up the execution environment

		final DataStream<List<CustomString>> text = env.addSource(
			new MultiDistRateControlledSourceFunction(
				sentenceSize,
				generators))
			.uid("sentence-source")
			.setParallelism(p[0]);

		// split up the lines in pairs (2-tuples) containing:
		// (word,1)
		DataStream<RecordWrapper<Tuple2<CustomString, Long>>> counts = text
			.flatMap(new WordCountUtils.CustomStringTokenizer())
			.name("Splitter FlatMap")
			.uid("flatmap")
			.setParallelism(p[1])
			.setMaxParallelism(maxParallelism)
			.keyBy(new KeySelector<RecordWrapper<Tuple2<CustomString, Long>>, CustomString>() {
				@Override
				public CustomString getKey(RecordWrapper<Tuple2<CustomString, Long>> value) throws Exception {
					return value.getRecord().f0;
				}
			})
			.process(new WordCountUtils.WordCountAggregator(windowSize))
			.name("Count")
			.uid("count")
			.setParallelism(p[2])
			.setMaxParallelism(maxParallelism);
//			.setVirtualNodesNum(numOfVirtualNodes);

		counts.addSink(new LatencySinkFunction<>(System.currentTimeMillis()+"_latency-sink", latenciesOutputPath, maxParallelism, p[2], numOfVirtualNodes))
			.name("Latency Sink")
			.uid("latency-sink")
			.setParallelism(p[2]);

//		counts.writeAsCsv(latenciesOutputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(p[2]);

		return env.getStreamGraph().getJobGraph();
	}

	public static List<RandomSentenceGenerator> createGeneratorsStrategy(PropertiesHandler propertiesHandler) {
		String prefix = "generators.";
		int i = 1;
		List<RandomSentenceGenerator> strategies = new ArrayList<>();

		do {
			strategies.add(SentenceGeneratorFactory.createInstance(prefix + i, propertiesHandler));
			i++;
		}
		while (propertiesHandler.getProperty(prefix + i + ".duration") != null);

		return strategies;
	}

	private static void configureEnvironment(StreamExecutionEnvironment env, int defaultParallelism, int maxParallelism, int checkpointingInterval, boolean cancelState) {


		// Rhino Job config
		env.setParallelism(defaultParallelism);
		if (0 < maxParallelism) {
			env.getConfig().setMaxParallelism(maxParallelism);
		}
		env.enableCheckpointing(checkpointingInterval);
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.getCheckpointConfig().setFailOnCheckpointingErrors(true);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointingInterval);
		env.getConfig().setUseSnapshotCompression(true);

		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		if (!cancelState) {
			env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		}

	}

}



