package org.apache.flink.morpheus.generators;

import org.apache.flink.morpheus.generators.utils.dists.LinearWeightedUniformDistribution;
import org.apache.flink.morpheus.generators.utils.dists.NormalDistribution;
import org.apache.flink.morpheus.generators.utils.dists.RandomDistribution;
import org.apache.flink.morpheus.generators.utils.dists.RoundRobinDistribution;
import org.apache.flink.morpheus.generators.utils.dists.SimpleWeightedUniformDistribution;
import org.apache.flink.morpheus.generators.utils.dists.UniformDistribution;
import org.apache.flink.morpheus.utils.PropertiesHandler;

import java.util.Arrays;
import java.util.stream.IntStream;

public class SentenceGeneratorFactory {

	public static RandomSentenceGenerator createInstance(String prefix, PropertiesHandler args) {

		RandomSentenceGenerator.GenType type = RandomSentenceGenerator.GenType.valueOf(args.getProperty(prefix + ".type"));
		int wordLength = args.getInteger(prefix + ".word-length", args.getInteger("word-length"));
		long duration = args.getLong(prefix + ".duration", Long.MAX_VALUE);
		int rate = args.getInteger(prefix + ".source-rate", args.getInteger("source-rate"));
		RandomSentenceGenerator generator;


		switch (type) {
			case UNIFORM:
				return createUniformGenerator(duration, rate, wordLength, prefix, args);
			case NORMAL:
				return createNormalGenerator(duration, rate, wordLength, prefix, args);
			case CUSTOM:
				return createCustomGenerator(duration, rate, wordLength, prefix, args);
			default:
				return new NoOpSentenceGenerator();
		}
	}

	private static RandomSentenceGenerator createCustomGenerator(long duration, int rate, int wordLength, String prefix, PropertiesHandler args) {
		RandomDistribution opDistribution;
		RandomDistribution vNodeDistribution;
		RandomDistribution wordDistribution;

		opDistribution = createDist(prefix + ".op", args);
		vNodeDistribution = createDist(prefix + ".v", args);
		wordDistribution = createDist(prefix + ".w", args);

		return new CustomDistSentenceGenerator(duration, rate, wordLength, args.getInteger(prefix + ".op.max-parallelism",
			args.getInteger("max-parallelism")), opDistribution, vNodeDistribution, wordDistribution);
	}

	private static RandomDistribution createDist(String prefix, PropertiesHandler args) {

		RandomDistribution val = (RandomDistribution) args.getModuleProperties().get(prefix + ".dist-instance");
		if (val != null) {
			return val;
		}

		RandomDistribution.Type opDist = RandomDistribution.Type.valueOf(args.getProperty(prefix + ".dist", "UNIFORM"));

		int bound = args.getInteger(prefix + ".dist.bound");

		switch (opDist) {
			case NORMAL:
				int opBias = args.getInteger(prefix + ".dist.bias");
				double opSkew = args.getDouble(prefix + ".dist.skew");

				return new NormalDistribution(bound, opBias, opSkew);
			case LINEAR_WEIGHTED:
				int constant = args.getInteger(prefix + ".dist.constant", 1);
				int slope = args.getInteger(prefix + ".dist.slope");
				return new LinearWeightedUniformDistribution(slope, constant, bound);
			case SIMPLE_WEIGHTED:
				int[] weights = args.getIntegerArray(prefix + ".dist.weights", IntStream.generate(() -> 1).limit(args.getInteger("parallelism", 1)).toArray());
				return new SimpleWeightedUniformDistribution(bound, weights);
			case MASKED:
				throw new UnsupportedOperationException();
			case ROUND_ROBIN:
				return new RoundRobinDistribution(bound);
			default:
				return new UniformDistribution(bound);

		}
	}


	private static NormalDistSentenceGenerator createNormalGenerator(long duration, int rate, int wordLength, String prefix, PropertiesHandler args) {
		double skew = args.getDouble(prefix + ".skew", 0.01);
		int bias = args.getInteger(prefix + ".bias", 0);

		return new NormalDistSentenceGenerator(wordLength, duration, rate, bias, skew);
	}

	private static RandomSentenceGenerator createUniformGenerator(long duration, int rate, int wordLength, String prefix, PropertiesHandler args) {
		double skewProbability = args.getDouble(prefix + ".skewProbability", 0.0);
		return new UniformSentenceGenerator(wordLength, duration, rate, skewProbability);
	}

}
