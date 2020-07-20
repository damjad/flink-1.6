package org.apache.flink.morpheus.wordcount;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.morpheus.wordcount.generators.RandomSentenceGenerator;
import org.apache.flink.morpheus.wordcount.generators.utils.vo.CustomString;
import org.apache.flink.morpheus.wordcount.generators.utils.vo.RecordWrapper;
import org.apache.flink.morpheus.wordcount.kafka.CustomStringDeserializer;
import org.apache.flink.morpheus.wordcount.sinks.LatencySinkFunction;
import org.apache.flink.morpheus.wordcount.sources.MultiDistRateControlledSourceFunction;
import org.apache.flink.morpheus.wordcount.utils.PropertiesHandler;
import org.apache.flink.morpheus.wordcount.utils.WordCountUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.apache.flink.morpheus.wordcount.generators.SentenceGeneratorFactory.createGeneratorsStrategy;

public class SkewWordCount {

	static SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy_HH:mm:ss");
	static String date = formatter.format(new Date());


	public static void main(String[] args) throws Exception {

		// Checking input parameters
		ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		PropertiesHandler props = PropertiesHandler.getInstance(params.get("properties-file", "src/main/skew-word-count.properties"));

		params = params.mergeWith(ParameterTool.fromPropertiesFile(props.getFilePath()));

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		backup(props);

		createWordCountJobGraph(env,
			props,
			params.getInt("p1", props.getInteger("source-parallelism", props.getInteger("default-parallelism", 1))),
			params.getInt("p2", props.getInteger("parallelism", props.getInteger("default-parallelism", 1))));

		env.execute();

	}

//	public static void cleanup(PropertiesHandler props) throws IOException {
//		// remove metrics manager metrics
//		FileUtils.cleanDirectory(new File(props.getProperty("policy.rates.path")));
//		FileUtils.cleanDirectory(new File(props.getProperty("latencies-path")));
//
//	}

	public static boolean backup(PropertiesHandler props) throws IOException {
		// remove metrics manager metrics
		boolean x = true;

		String lastDate = "x";


		File lastDateFile = new File(props.getProperty("last-date-path"));
		if (lastDateFile.exists()) {
			lastDate = Files.readAllLines(lastDateFile.toPath(), Charset.defaultCharset()).get(0);
		}
		else {
			lastDateFile.getParentFile().mkdirs();
		}
		Files.write(lastDateFile.toPath(), date.getBytes(Charset.defaultCharset()), CREATE, TRUNCATE_EXISTING);

		File latencies = new File(props.getProperty("latencies-path"));
		File newLatencies = new File(latencies.getAbsolutePath() + "_latencies_" + lastDate);
		if (latencies.exists()) {
			FileUtils.copyDirectory(latencies, newLatencies);
			FileUtils.cleanDirectory(latencies);
		}
		else {
			x = x && latencies.mkdirs();
		}


		File rates = new File(props.getProperty("policy.rates.path"));
		File newRates = new File(rates.getAbsolutePath() + "_rates_" + lastDate);
		if (rates.exists()) {

			FileUtils.copyDirectory(rates, newRates);
			FileUtils.cleanDirectory(rates);
		}
		else {
			x = x && rates.mkdirs();
		}

		File backupDir = new File(props.getProperty("backup-path"));
		File backsupSubDir = new File(backupDir.getAbsolutePath() + "/" + latencies.getName() + "_" + lastDate);
		if (newLatencies.exists()) FileUtils.moveDirectoryToDirectory(newLatencies, backsupSubDir, true);
		if (newRates.exists()) FileUtils.moveDirectoryToDirectory(newRates, backsupSubDir, true);


		return x;
	}

	public static JobGraph createWordCountJobGraph(StreamExecutionEnvironment env,
												   PropertiesHandler props,
												   int... p) {

		List<RandomSentenceGenerator> generators = createGeneratorsStrategy(props);
		//Warmup.
//		generators.forEach(x -> x.nextSentence(1));


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

		final DataStream<RecordWrapper<Tuple2<CustomString, Long>>> text;

		if (props.getBoolean("autogen", true)) {
			text = env.addSource(
				new MultiDistRateControlledSourceFunction(
					sentenceSize,
					generators))
				.uid("sentence-source")
				.setParallelism(p[0])
				.flatMap(new WordCountUtils.CustomStringTokenizer())
				.name("Splitter FlatMap")
				.uid("flatmap")
				.setParallelism(p[0])
				.setMaxParallelism(maxParallelism);
		}
		else {
			FlinkKafkaConsumer011<CustomString[]> kafkaWords =
				new FlinkKafkaConsumer011<>(props.getProperty("topic", "words"),
					new CustomStringDeserializer(), props.getModuleProperties());

			kafkaWords.setCommitOffsetsOnCheckpoints(true);
			if (props.getBoolean("start_earliest", false)) {
				kafkaWords.setStartFromEarliest();
			}
			else {
				kafkaWords.setStartFromLatest();
			}

			text = env
				.addSource(kafkaWords)
				.setParallelism(p[0])
				.uid("sentence-source")
				.returns(TypeInformation.of(new TypeHint<CustomString[]>() {
				}))
				.flatMap(new WordCountUtils.CustomStringTokenizer2())
				.name("Splitter FlatMap")
				.uid("flatmap")
				.setParallelism(p[0])
				.setMaxParallelism(maxParallelism)
				.returns(TypeInformation.of(new TypeHint<RecordWrapper<Tuple2<CustomString, Long>>>() {
				}));

		}

		// split up the lines in pairs (2-tuples) containing:
		// (word,1)
		DataStream<RecordWrapper<Tuple2<CustomString, Long>>> counts = text
			.keyBy(new KeySelector<RecordWrapper<Tuple2<CustomString, Long>>, CustomString>() {
				@Override
				public CustomString getKey(RecordWrapper<Tuple2<CustomString, Long>> value) throws Exception {
					return value.getRecord().f0;
				}
			})
			.process(new WordCountUtils.WordCountAggregator(windowSize))
			.name("Count")
			.uid("count")
			.setParallelism(p[1])
			.setMaxParallelism(maxParallelism);

		counts.addSink(new LatencySinkFunction<>("latency-sink", latenciesOutputPath, maxParallelism, p[1], numOfVirtualNodes))
			.name("Latency Sink")
			.uid("latency-sink")
			.setParallelism(p[1]);

//		counts.writeAsCsv(latenciesOutputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(p[2]);

		return env.getStreamGraph().getJobGraph();
//		return null;
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



