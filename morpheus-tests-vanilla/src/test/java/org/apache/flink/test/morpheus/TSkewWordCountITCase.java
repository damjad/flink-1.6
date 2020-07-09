package org.apache.flink.test.morpheus;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.morpheus.utils.PropertiesHandler;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.morpheus.SkewWordCount.cleanup;
import static org.apache.flink.morpheus.SkewWordCount.createWordCountJobGraph;
import static org.junit.Assert.fail;

public class TSkewWordCountITCase extends TBaseTest {

	private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(TSkewWordCountITCase.class);

	// Job Config.
	private static final int WORD_LENGTH = 3;


	@Test
	public void testSimpleWordCount() throws Exception {

		FiniteDuration timeout = new FiniteDuration(1, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		JobID jobID = null;
		ActorGateway jobManager = null;
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		String[] args = {"--policy.rates.path", "/home/danish/FastWorkspace/BDMA/TUB/ds2/ds2/controller/running-examples/wordcount/metrics_repo/"};

//		ParameterTool params = ParameterTool.fromArgs(args);
		// make parameters available in the web interface
		PropertiesHandler props = PropertiesHandler.getInstance("/home/danish/FastWorkspace/BDMA/TUB/flink-1.6.0/flink-1.6/flink/morpheus-tests-vanilla/src/main/conf/load-balance-skew-word-count.properties");
		env.getConfig().setGlobalJobParameters(ParameterTool.fromPropertiesFile(props.getFilePath()));
		cleanup(props);

		try {
			jobManager = cluster.getLeaderGateway(deadline.timeLeft());

			JobGraph jobGraph = createWordCountJobGraph(env,
				props,
				1,
				1,
				props.getInteger("parallelism", 1),
				props.getInteger("parallelism", 1));

			jobID = jobGraph.getJobID();

			cluster.submitJobAndWait(jobGraph, true, timeout);

		}
		finally {
			// clear the CollectionSink set for the restarted job

			// clear any left overs from a possibly failed job
			if (jobID != null && jobManager != null) {
				Future<Object> jobRemovedFuture = jobManager.ask(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobID), timeout);

				try {
					Await.ready(jobRemovedFuture, timeout);
				}
				catch (TimeoutException | InterruptedException ie) {
					fail("Failed while cleaning up the cluster.");
				}
			}
		}
	}


}
