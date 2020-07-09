package org.apache.flink.test.morpheus;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.fail;

public class TWordCountITCase extends TBaseTest {


	@Test
	public void testSimpleWordCount() throws Exception {
		FiniteDuration timeout = new FiniteDuration(1, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		JobID jobID = null;
		ActorGateway jobManager = null;

		try {
			jobManager = cluster.getLeaderGateway(deadline.timeLeft());

			JobGraph jobGraph = createWordCountJobGraph(2, 4096, 20_000, true);

			jobID = jobGraph.getJobID();

			cluster.submitJobAndWait(jobGraph, true, timeout);


		} finally {
			// clear the CollectionSink set for the restarted job

			// clear any left overs from a possibly failed job
			if (jobID != null && jobManager != null) {
				Future<Object> jobRemovedFuture = jobManager.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobID), timeout);

				try {
					Await.ready(jobRemovedFuture, timeout);
				} catch (TimeoutException | InterruptedException ie) {
					fail("Failed while cleaning up the cluster.");
				}
			}
		}
	}


	private static JobGraph createWordCountJobGraph(
		int parallelism,
		int maxParallelism,
		int checkpointingInterval,
		boolean cancelState) {

		int numOfVirtualNodes = 4;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
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

		//TODO: Solve dependency issue.
//
//		DataStream<String> text = env.fromElements(WordCountData.WORDS);
//			// split up the lines in pairs (2-tuples) containing: (word,1)
//			text.flatMap(new Tokenizer())
//				.setVirtualNodesNum(numOfVirtualNodes)
//				.setReplicaSlotsHint(4)
//				// group by the tuple field "0" and sum up tuple field "1"
//				.keyBy(0).sum(1)
//				.addSink(new SinkFunction<Tuple2<String, Integer>>() {
//					@Override
//					public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
//						System.out.println(value);
//					}
//				});


		return env.getStreamGraph().getJobGraph();
	}

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}
}
