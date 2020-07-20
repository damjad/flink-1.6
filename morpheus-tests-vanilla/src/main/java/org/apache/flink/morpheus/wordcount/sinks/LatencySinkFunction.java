package org.apache.flink.morpheus.wordcount.sinks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.LongContainerGauge;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.morpheus.wordcount.generators.utils.vo.RecordWrapper;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.ShutdownHookUtil;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystemException;

public class LatencySinkFunction<T> extends RichSinkFunction<RecordWrapper<T>> {

	private static final Logger LOG = LoggerFactory.getLogger(LatencySinkFunction.class);

	private static final long LATENCY_THRESHOLD = 10L * 60L * 1000L;

	private transient SummaryStatistics sinkEventLatency;
	private transient SummaryStatistics sinkProcessingLatency;

	private transient BufferedWriter writer;

	private transient StringBuilder stringBuffer;

	private transient int index;

	private transient boolean logInit = false;

	private transient int writtenSoFar = 0;


	private final String name;
	private final String filePath;

	// for vnode
	private final int maxParallelism;
	private final int parallelism;
	private final int numVNodes;


	/**
	 * these counters will be used inside StreamInputProcessor and will be reset for every buffer flush.
	 */
	private transient LongContainerGauge procLatencyDur;
	private transient LongContainerGauge eventLatencyDur;
	private transient SimpleCounter procLatencyCount;
	private transient SimpleCounter eventLatencyCount;
	private transient LongContainerGauge procLatency;
	private transient LongContainerGauge eventLatency;


	public LatencySinkFunction(String name, String filePath, int maxParallelism, int parallelism, int numVNodes) {
		this.name = name;
		this.filePath = filePath;
		this.maxParallelism = maxParallelism;
		this.parallelism = parallelism;
		this.numVNodes = numVNodes;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
//		super.open(parameters);

		this.sinkEventLatency = new SummaryStatistics();
		this.sinkProcessingLatency = new SummaryStatistics();
		this.stringBuffer = new StringBuilder(2048);
		this.index = getRuntimeContext().getIndexOfThisSubtask();

		File logDir = new File(filePath);
		File logSubDir = new File(logDir, name + "_" + index);
		if (!logSubDir.exists()) {
			if (!logSubDir.mkdirs()) {
				throw new FileSystemException("Could not create directory: " + logSubDir.getAbsolutePath());
			}
		}
		File logFile = new File(logSubDir, name + "_" + index + ".csv");

		if (logFile.exists()) {
			this.writer = new BufferedWriter(new FileWriter(logFile, true));
			this.writer.write("\n");
		}
		else {
			this.writer = new BufferedWriter(new FileWriter(logFile, false));
			stringBuffer.append("subtask,vnodeId,ts,latencyCount,flightTimeCount,latencyMean,flightTimeMean,latencyMin,flightTimeMin,latencyMax,flightTimeMax");
			stringBuffer.append("\n");
			writer.write(stringBuffer.toString());
			writtenSoFar += stringBuffer.length() * 2;
		}

		ShutdownHookUtil.addShutdownHook(writer, getRuntimeContext().getTaskNameWithSubtasks(), LOG);

		stringBuffer.setLength(0);
		logInit = true;

		procLatencyDur = new LongContainerGauge();
		eventLatencyDur = new LongContainerGauge();
		procLatencyCount = new SimpleCounter();
		eventLatencyCount = new SimpleCounter();
		getRuntimeContext().getMetricGroup().gauge("procLatencyDur", procLatencyDur);
		getRuntimeContext().getMetricGroup().counter("procLatencyCount", procLatencyCount);
		getRuntimeContext().getMetricGroup().gauge("eventLatencyDur", eventLatencyDur);
		getRuntimeContext().getMetricGroup().counter("eventLatencyCount", eventLatencyCount);

		procLatency = new LongContainerGauge();
		eventLatency = new LongContainerGauge();
		getRuntimeContext().getMetricGroup().gauge("procLatency", procLatency);
		getRuntimeContext().getMetricGroup().gauge("eventLatency", eventLatency);
	}

	@Override
	public void close() throws Exception {
//		super.close();
		if (logInit) {
//			updateCSV(System.currentTimeMillis());
			writer.flush();
			writer.close();
		}

		sinkProcessingLatency.clear();
		sinkEventLatency.clear();
	}

	private void updateCSV(long timestamp, int vnodeId) throws IOException {
		try {
			stringBuffer.append(index);
			stringBuffer.append(",");
			stringBuffer.append(vnodeId);
			stringBuffer.append(",");
			stringBuffer.append(timestamp);
			stringBuffer.append(",");

			stringBuffer.append(sinkEventLatency.getN());
			stringBuffer.append(",");
			stringBuffer.append(sinkProcessingLatency.getN());
			stringBuffer.append(",");

			stringBuffer.append(sinkEventLatency.getMean());
			stringBuffer.append(",");
			stringBuffer.append(sinkProcessingLatency.getMean());
			stringBuffer.append(",");
//				stringBuffer.append(sinkLatencyWindow.getMean());
//				stringBuffer.append(",");

//				stringBuffer.append(sinkLatencyBid.getStandardDeviation());
//				stringBuffer.append(",");
//				stringBuffer.append(sinkLatencyFlightTime.getStandardDeviation());
//				stringBuffer.append(",");


			stringBuffer.append(sinkEventLatency.getMin());
			stringBuffer.append(",");
			stringBuffer.append(sinkProcessingLatency.getMin());
			stringBuffer.append(",");

			stringBuffer.append(sinkEventLatency.getMax());
			stringBuffer.append(",");
			stringBuffer.append(sinkProcessingLatency.getMax());

			stringBuffer.append("\n");

			writer.write(stringBuffer.toString());

			writtenSoFar += stringBuffer.length() * 2;
			if (writtenSoFar >= (8 * 1024 * 1024)) {
				writer.flush();
				writtenSoFar = 0;
			}

		}
		finally {
			stringBuffer.setLength(0);
		}
	}

	@Override
	public void invoke(RecordWrapper<T> record, Context context) throws Exception {
		long timeMillis = context.currentProcessingTime();
		long latency = timeMillis - record.getEventTime();
		if (latency <= LATENCY_THRESHOLD) {
			sinkEventLatency.addValue(latency);
			sinkProcessingLatency.addValue(timeMillis - record.getIngestionTime());

			// for every value submitted to MetricsManager, It is reset.
//			assert 0 == eventLatencyDur.getValue();
//			assert 0 == procLatencyDur.getValue();
//			assert 0 == eventLatencyCount.getCount();
//			assert 0 == eventLatencyCount.getCount();

			eventLatencyDur.setValue(latency);
			procLatencyDur.setValue(timeMillis - record.getIngestionTime());
			eventLatencyCount.dec(eventLatencyCount.getCount() - 1);
			procLatencyCount.dec(procLatencyCount.getCount() - 1);


			eventLatency.setValue(((long) sinkEventLatency.getMean()));
			procLatency.setValue(((long) sinkProcessingLatency.getMean()));
//				sinkLatencyWindow.addValue(timeMillis - record.windowTriggeringTimestamp);
//			updateCSV(timeMillis, VNodeUtils.getVNode(record.getKey(), maxParallelism, numVNodes, parallelism));
			updateCSV(timeMillis, 0);
		}
	}
}

