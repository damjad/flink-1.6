package org.apache.flink.morpheus.wordcount.kafka;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.morpheus.wordcount.generators.RandomSentenceGenerator;
import org.apache.flink.morpheus.wordcount.generators.utils.vo.CustomString;
import org.apache.flink.morpheus.wordcount.utils.PropertiesHandler;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.morpheus.wordcount.SkewWordCount.createGeneratorsStrategy;


public class CustomStringKafkaGenerator {
	private static final Logger LOG = LoggerFactory.getLogger(CustomStringKafkaGenerator.class);

	public static void main(String[] args) throws IOException {
		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		PropertiesHandler props = PropertiesHandler.getInstance(params.get("properties-file", "src/main/skew-word-count.properties"));

		List<RandomSentenceGenerator> generators = createGeneratorsStrategy(props);

		ProducerGroup producerGroup = new ProducerGroup(generators);

		LOG.info("Producer started...");
		producerGroup.execute();

		try {
			Thread.sleep(1000);
		}
		catch (InterruptedException ie) {


		}
	}
}

class ProducerGroup {

	private static final Logger LOG = LoggerFactory.getLogger(ProducerGroup.class);


	private List<ProducerThread> producers;
	private ExecutorService pool;

	public ProducerGroup(List<RandomSentenceGenerator> generators) throws IOException {
		int numberOfProducers = Integer.parseInt(PropertiesHandler.getInstance().getModuleProperties().getProperty("source-parallelism"));
		producers = new ArrayList<>(numberOfProducers);

		int maxNumberOfMessages = Integer.parseInt(PropertiesHandler.getInstance().getModuleProperties().getProperty("total-sentences"));


		int maxMessagesPerThread = Math.floorDiv(maxNumberOfMessages, numberOfProducers);
		int remMpThread = maxMessagesPerThread % numberOfProducers;

		for (int id = 0; id < numberOfProducers; id++) {
			ProducerThread npThread;
			int numMessages = id < remMpThread ? maxMessagesPerThread + 1 : maxMessagesPerThread;
			npThread = new ProducerThread(id, numMessages, generators);
			producers.add(npThread);
		}

		pool = Executors.newFixedThreadPool(numberOfProducers);

		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				LOG.info("Shutdown hook invoked...");
				System.out.println("shutdown hook");
				for (int i = 0; i < producers.size(); i++) {
					ProducerThread producerThread = producers.get(i);
					producerThread.getInternalProducer().flush();
					producerThread.getInternalProducer().close();

				}
				pool.shutdownNow();

			}
		});

		try {
			Thread.sleep(500);
		}
		catch (InterruptedException e) {
			LOG.debug("{} interrupted", Thread.currentThread().getName());
		}
	}

	public void execute() {
		for (int i = 0; i < producers.size(); i++) {
			ProducerThread ncThread = producers.get(i);
			pool.submit(ncThread);
		}
	}
}

class ProducerThread implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(ProducerThread.class);

	private final KafkaProducer<Integer, CustomString[]> producer;
	private final String topic;
	private final Integer producerNumber;
	private final List<RandomSentenceGenerator> generators;
	private final Properties props;
	private final int numberOfMessages;
	private final int sentenceSize;

	public ProducerThread(int producerNumber, int numberOfMessages, List<RandomSentenceGenerator> generators) throws IOException {
		this.props = PropertiesHandler.getInstance().getModuleProperties();
		this.numberOfMessages = numberOfMessages;
		this.producer = new KafkaProducer<>(props, new IntegerSerializer(), new CustomStringSerializer());
		this.topic = props.getProperty("topic");
		this.producerNumber = producerNumber;
		this.generators = generators;
		this.sentenceSize = PropertiesHandler.getInstance().getInteger("sentence-size");
	}

	@Override
	public void run() {
		Iterator<RandomSentenceGenerator> it = generators.iterator();
		Long lastStartTime = System.currentTimeMillis();

		RandomSentenceGenerator gen = it.next();
		RateLimiter rateLimiter = RateLimiter.create(gen.getRate());

		int sentencesSent = 0;

		while (sentencesSent < numberOfMessages) {
			long emitStartTime = System.currentTimeMillis();
			if (emitStartTime > lastStartTime + gen.getDuration() && it.hasNext()) {
				lastStartTime = emitStartTime;
				gen = it.next();
				rateLimiter.setRate(gen.getRate());
			}

			rateLimiter.acquire();

			List<CustomString> sentence = gen.nextSentence(sentenceSize);
			if (null != sentence && !sentence.isEmpty()) {
				try {
					producer.send(new ProducerRecord<>(topic, producerNumber, sentence.toArray(new CustomString[0])));
					sentencesSent++;
				}
				catch (Exception ex) {
					System.out.println(ex);
					LOG.error("An error occurred while sending the data in kafka client", ex);
					break;
				}

			}
		}

	}

	public KafkaProducer<Integer, CustomString[]> getInternalProducer() {
		return producer;
	}
}
