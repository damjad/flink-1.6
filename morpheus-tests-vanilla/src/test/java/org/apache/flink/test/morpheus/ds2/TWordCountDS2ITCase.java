//package org.apache.flink.test.morpheus.ds2;
//
//import org.apache.flink.api.common.JobID;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.common.functions.RichFlatMapFunction;
//import org.apache.flink.api.common.state.ReducingState;
//import org.apache.flink.api.common.state.ReducingStateDescriptor;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.runtime.instance.ActorGateway;
//import org.apache.flink.runtime.jobgraph.JobGraph;
//import org.apache.flink.runtime.messages.JobManagerMessages;
//import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.test.morpheus.TBaseTest;
//import org.apache.flink.util.Collector;
//
//import org.junit.Test;
//
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//import java.util.function.BiConsumer;
//
//import scala.concurrent.Await;
//import scala.concurrent.Future;
//import scala.concurrent.duration.Deadline;
//import scala.concurrent.duration.FiniteDuration;
//
//import static org.apache.flink.morpheus.WordCountDS2.createWordCountJobGraph;
//import static org.junit.Assert.fail;
//
//public class TWordCountDS2ITCase extends TBaseTest  {
//
//	private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(TWordCountDS2ITCase.class);
//
//	private static int scalingFactor = 4;
//	private static int parallelism = 2;
//
//	@Test
//	public void testSimpleWordCount() throws Exception {
//		FiniteDuration timeout = new FiniteDuration(1, TimeUnit.MINUTES);
//		Deadline deadline = timeout.fromNow();
//
//		JobID jobID = null;
//		ActorGateway jobManager = null;
//
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//		String[] args = {"--policy.rates.path", "/home/danish/FastWorkspace/BDMA/TUB/ds2/ds2/controller/running-examples/wordcount/metrics_repo/"};
//
//		ParameterTool params = ParameterTool.fromArgs(args);
//		// make parameters available in the web interface
//		env.getConfig().setGlobalJobParameters(params);
//
//
//		try {
//			jobManager = cluster.getLeaderGateway(deadline.timeLeft());
//
//			JobGraph jobGraph = createWordCountJobGraph(env,
//				80000,
//				100,
//				1,
//				4096,
//				4,
//				20_000,
//				true,
//				1,
//				1,
//				parallelism,
//				parallelism);
//
//			jobID = jobGraph.getJobID();
//
//			Thread daemon = createDummyRescaleDaemon(jobID);
//			daemon.start();
//
//			cluster.submitJobAndWait(jobGraph, true, timeout);
//
//
//		}
//		finally {
//			// clear the CollectionSink set for the restarted job
//
//			// clear any left overs from a possibly failed job
//			if (jobID != null && jobManager != null) {
//				Future<Object> jobRemovedFuture = jobManager.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobID), timeout);
//
//				try {
//					Await.ready(jobRemovedFuture, timeout);
//				}
//				catch (TimeoutException | InterruptedException ie) {
//					fail("Failed while cleaning up the cluster.");
//				}
//			}
//		}
//	}
//
//	private Thread createDummyRescaleDaemon(JobID jobID) {
//		Thread t = new Thread(new Runnable() {
//			@Override
//			public void run() {
//				try {
//					Thread.sleep(15_000);
//
//					CompletableFuture<JobManagerMessages.TopologyChangeResult> rescalingFuture = client.rescaleOperator(jobID, "Count -> Latency Sink", scalingFactor, new int[]{-1}, new int[]{0});
//
//					rescalingFuture.whenComplete(new BiConsumer<JobManagerMessages.TopologyChangeResult, Throwable>() {
//						@Override
//						public void accept(JobManagerMessages.TopologyChangeResult result, Throwable throwable) {
//							if (result.error() == null) {
//								LOG.info("all good");
//							}
//							else if (throwable != null) {
//								LOG.error("error", throwable);
//							}
//							else {
//								LOG.info("not good");
//							}
//						}
//					});
//
//				}
//				catch (Throwable ex) {
//					LOG.error("Error", ex);
//				}
//			}
//		});
//		t.setDaemon(true);
//
//		return t;
//	}
//
//	// *************************************************************************
//	// USER FUNCTIONS
//	// *************************************************************************
//
//	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {
//		private static final long serialVersionUID = 1L;
//
//		@Override
//		public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
//			// normalize and split the line
//			String[] tokens = value.toLowerCase().split("\\W+");
//
//			// emit the pairs
//			for (String token : tokens) {
//				if (token.length() > 0) {
//					out.collect(new Tuple2<>(token, 1L));
//				}
//			}
//		}
//	}
//
//	public static final class CountWords extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
//
//		private transient ReducingState<Long> count;
//
//		@Override
//		public void open(Configuration parameters) throws Exception {
//
//			ReducingStateDescriptor<Long> descriptor =
//				new ReducingStateDescriptor<Long>(
//					"count", // the state name
//					new Count(),
//					BasicTypeInfo.LONG_TYPE_INFO);
//
//			count = getRuntimeContext().getReducingState(descriptor);
//		}
//
//		@Override
//		public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, Long>> out) throws Exception {
//			count.add(value.f1);
//			out.collect(new Tuple2<>(value.f0, count.get()));
//		}
//
//		public static final class Count implements ReduceFunction<Long> {
//
//			@Override
//			public Long reduce(Long value1, Long value2) throws Exception {
//				return value1 + value2;
//			}
//		}
//	}
//}
