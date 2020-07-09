//package org.apache.flink.morpheus.generators.tests;
//
//import org.apache.flink.morpheus.generators.utils.vo.CustomString;
//import org.apache.flink.morpheus.generators.utils.RandUtils;
//import org.apache.flink.morpheus.generators.utils.WordUtils;
//import org.apache.flink.runtime.morpheus.util.profiling.VNodeUtils;
//
//import org.apache.commons.math3.util.ArithmeticUtils;
//import org.knowm.xchart.BitmapEncoder;
//import org.knowm.xchart.CategoryChart;
//import org.knowm.xchart.CategoryChartBuilder;
//import org.knowm.xchart.CategorySeries;
//import org.knowm.xchart.XYChart;
//import org.knowm.xchart.XYChartBuilder;
//import org.knowm.xchart.XYSeries;
//import org.knowm.xchart.style.Styler;
//
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Comparator;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.TimeUnit;
//import java.util.function.BiFunction;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//import java.util.stream.Stream;
//
//
///**
// * TODO: 1. Numbers Distribution
// * 2. V Node Distribution
// * 3. Hashes Distribution
// */
//public class DistributionSimulator {
//
//	private static final Integer nVirtualNodes = 4;
//	private static final List<Integer> parallelismList = Arrays.asList(2, 3, 4, 5, 6);
//	private static Integer maxParallelism;
//
//	private static final Integer numberOfWords = 100_000;
//	private static final Integer maxWordSize = 4;
//
//	private static final Integer maxSkew = 15;
//
//
//	private static final Integer simulatorThreads = 16;
//	private static final BiFunction<Integer, Integer, Integer> incrementer = (Integer x, Integer y) -> y == null ? 1 : y + 1;
//
//
//	public static void main(String[] args) throws InterruptedException {
//
//		maxParallelism = nVirtualNodes * parallelismList.stream().reduce(ArithmeticUtils::lcm).orElse(1);
//
//		String dirPath = "/home/danish/FastWorkspace/BDMA/TUB/flink-1.6.0/flink-1.6/flink/morpheus-tests-vanilla/distribution-stats/";
//
//		ExecutorService executor = Executors.newFixedThreadPool(simulatorThreads);
////
////		for (int wordSize = 2; wordSize <= maxWordSize; wordSize++) {
////			List<String> words = WordUtils.genWords(wordSize);
////			List<TCustomString> wordsList = IntStream.range(0, words.size())
////				.mapToObj(x -> new TCustomString(words.get(x), x))
////				.collect(Collectors.toList());
////
////			final int wordS = wordSize;
////
////			executor.submit(() -> testUniformDistributionNumbers(wordsList, dirPath, wordS));
////
////			for (int skew = 5; skew <= maxSkew; skew = skew + 5) {
////				final int sk = skew;
////				executor.submit(() -> testNormalDistributionNumbers(wordsList, dirPath, wordS, sk));
////			}
////		}
//
//
//		for (int wordSize = 2; wordSize <= maxWordSize; wordSize++) {
//
//			if (Math.pow(26, wordSize) < maxParallelism) {
//				System.out.println("Skipping word size " + wordSize + " because word list size " + Math.pow(26, wordSize) + " is less than max parallelsim " + maxParallelism);
//				continue;
//			}
//
//			List<String> words = WordUtils.genWords(wordSize);
//			List<CustomString> wordsList = IntStream.range(0, words.size())
//				.mapToObj(x -> new CustomString(words.get(x), x))
//				.collect(Collectors.toList());
//
//			final int wordLength = wordSize;
//
//
//			for (double opDistSlope = 0; opDistSlope < 4; opDistSlope++) {
//				final double opSlope = opDistSlope;
//				executor.submit(() -> new DistributionSimulator().testCustomDistributionNumbers("custom", "uniform", "uniform", wordsList, dirPath, wordLength, opSlope, 0, 0));
////				new DistributionSimulator().testCustomDistributionNumbers("custom", "uniform", "uniform", wordsList, dirPath, wordLength, opSlope, 0, 0);
//			}
//			// uniform distribution every where.
//
//
////			for (String opDist : dists) {
////				boolean opDistIsUniform = isUniform(opDist);
////				int osk = opDistIsUniform ? 0 : 3;
////
////				do {
////					final int opSkew = osk;
////
////					for (String vDist : dists) {
////						boolean vNodeDistIsUniform = isUniform(vDist);
////						int vsk = vNodeDistIsUniform ? 0 : maxSkew;
////						do {
////							final int vNodeSkew = vsk;
////
////							for (String wDist : dists) {
////								boolean wordDistIsUniform = isUniform(wDist);
////								int wsk = wordDistIsUniform ? 0 : maxSkew;
////								do {
////									final int wordSkew = wsk;
////
////									executor.submit(() -> testCustomDistributionNumbers(opDistIsUniform, vNodeDistIsUniform, wordDistIsUniform, wordsList, dirPath, wordS, opSkew, vNodeSkew, wordSkew));
//////									testCustomDistributionNumbers(opDistIsUniform, vNodeDistIsUniform, wordDistIsUniform, wordsList, dirPath, wordS, opSkew, vNodeSkew, wordSkew);
////									wsk -= 5;
////								}
////								while (wsk > 5);
////
////								vsk -= 5;
////							}
////						}
////						while (vsk > 5);
////					}
////
////					osk = osk - 1;
////				}
////				while (osk >= 0);
////			}
//		}
//
//		Thread.sleep(3000);
//		executor.shutdown();
//
//		try {
//			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
//		}
//		catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//	}
//
//	private boolean isUniform(String type) {
//		return "uniform".equals(type);
//	}
//
//	private void testUniformDistributionNumbers(List<CustomString> wordsList, String dirPath, int wordLength) {
//		testDistributionNumbers("uniform", wordsList, dirPath, wordLength, -1);
//	}
//
//	private void testNormalDistributionNumbers(List<CustomString> wordsList, String dirPath, int wordLength, int skew) {
//		testDistributionNumbers("normal", wordsList, dirPath, wordLength, skew);
//	}
//
//	private void testCustomDistributionNumbers(String opDist, String vNodeDist, String wordsDist, List<CustomString> wordsList, String dirPath, int wordLength, double opSkew, int vNodeSkew, int wordSkew) {
//		int wordListSize = wordsList.size();
//		Map<Integer, Integer> numberMap = new HashMap<>();
//		Map<Integer, Integer> hashMap = new HashMap<>();
//		Map<Integer, Map<Integer, Integer>> vNodesMap = new HashMap<>();
//		parallelismList.forEach(p -> vNodesMap.put(p, new HashMap<>()));
//
//		for (Integer parallelism : parallelismList) {
//			int x = 45;
//
//			List<Integer> operatorIdsDist = new ArrayList<>();
//
//			for (int i = 0; i < numberOfWords; i++) {
//				int randOpId = getNextRandomOperatorId(opDist, operatorIdsDist, parallelism, opSkew, 1);
//				int randVNodeId = getRandomVNodeIdFromOperatorId(randOpId, parallelism, vNodeDist, vNodeSkew);
//
//				assert randOpId == randVNodeId % parallelism : "Wrong calculation of OpId & VNodeId";
//
//				int wordId = getRandomWordId(wordsDist, wordSkew, wordListSize, randVNodeId, parallelism);
//
//				numberMap.compute(wordId, incrementer);
//				try {
//					assert randVNodeId == getVNodeIdFromWord(wordsList.get(wordId), parallelism) : "Wrong calculation of VnodeId & WordId!";
//					hashMap.compute(wordsList.get(wordId).hashCode(), incrementer);
//				}
//				catch (Throwable t) {
//					System.out.println(t);
//					getVNodeIdFromWord(wordsList.get(wordId), parallelism);
//					throw t;
//				}
//
//				vNodesMap.get(parallelism).compute(randVNodeId, incrementer);
//			}
//		}
//
//		writeMapsToFiles("custom", dirPath, wordLength, numberMap, hashMap, vNodesMap, opDist, vNodeDist, wordsDist, opSkew, vNodeSkew, wordSkew);
//	}
//
//	private int getNextRandomOperatorId(String opDist, List<Integer> operatorIdsDist, Integer parallelism, double opSkew, double dcOffset) {
//		if ("custom".equals(opDist)) {
//			if (operatorIdsDist.isEmpty()) {
//				operatorIdsDist.addAll(IntStream.range(0, parallelism).boxed().
//					flatMap(x -> IntStream.range(0, (int) (dcOffset + x * opSkew)).boxed().map(y -> x)).
//					collect(Collectors.toList()));
//			}
//
//			return operatorIdsDist.get(RandUtils.nexUniformInt(operatorIdsDist.size()));
//		}
//
//		else {
//			return getNextRandom(opDist, parallelism, opSkew);
//		}
//	}
//
//
//	private void testDistributionNumbers(String type, List<CustomString> wordsList, String dirPath, int wordLength, int skew) {
//		int wordListSize = wordsList.size();
//		Map<Integer, Integer> numberMap = new HashMap<>();
//		Map<Integer, Integer> hashMap = new HashMap<>();
//		Map<Integer, Map<Integer, Integer>> vNodesMap = new HashMap<>();
//		parallelismList.forEach(p -> vNodesMap.put(p, new HashMap<>()));
//
//		for (int i = 0; i < numberOfWords; i++) {
//			int wordId = getNextRandom(type, wordListSize, skew);
//			numberMap.compute(wordId, incrementer);
//			hashMap.compute(wordsList.get(wordId).hashCode(), incrementer);
//
//			for (Integer parallelism : parallelismList) {
//				vNodesMap.get(parallelism).compute(getVNodeIdFromWord(wordsList.get(wordId), parallelism), incrementer);
//			}
//
//		}
//
//		writeMapsToFiles(type, dirPath, wordLength, numberMap, hashMap, vNodesMap, skew);
//	}
//
//	private void writeMapsToFiles(String type, String dirPath, int wordLength, Map<Integer, Integer> numberMap, Map<Integer, Integer> hashMap, Map<Integer, Map<Integer, Integer>> vNodesMap, Object... extraParams) {
//		// numberMap
//		writeMapToFile(dirPath + getName(type, wordLength, "number_map", extraParams) + ".csv", numberMap);
//		writeMapToImage(dirPath, getName(type, wordLength, "number_map", extraParams), numberMap, -1);
//
//		//hashMap
//		writeMapToFile(dirPath + getName(type, wordLength, "hashes_map", extraParams) + ".csv", hashMap);
//		writeMapToImage(dirPath, getName(type, wordLength, "hashes_map", extraParams), hashMap, -1);
//
//		for (Integer parallelism : vNodesMap.keySet()) {
//			//hashMap
//			writeMapToFile(dirPath + getName(type, wordLength, "vNode_map", parallelism, extraParams) + ".csv", vNodesMap.get(parallelism));
//			writeMapToImage(dirPath, getName(type, wordLength, "vNode_map", parallelism, extraParams), vNodesMap.get(parallelism), 16, true);
//
//			Map<Integer, Integer> opMap = new HashMap<>();
//
//			for (int opId = 0; opId < parallelism; opId++) {
//				for (int vNodeId = opId; vNodeId < parallelism * nVirtualNodes; vNodeId = vNodeId + parallelism) {
//					final int vNode = vNodeId;
//					opMap.compute(opId, (x, y) -> y == null ? vNodesMap.get(parallelism).getOrDefault(vNode, 0) : y + vNodesMap.get(parallelism).getOrDefault(vNode, 0));
//				}
//			}
//
//			writeMapToFile(dirPath + getName(type, wordLength, "op_map", parallelism, extraParams) + ".csv", opMap);
//			writeMapToImage(dirPath, getName(type, wordLength, "op_map", parallelism, extraParams), opMap, 16, true);
//
//		}
//	}
//
//	private String getName(String type, Object... arr) {
//		String argsData = Arrays.stream(arr)
//			.filter(Objects::nonNull)
//			.flatMap(x -> x instanceof Object[] ? Arrays.stream((Object[]) x) : Stream.of(x))
//			.map(Object::toString)
//			.collect(Collectors.joining("-"));
//		return type + "-" + argsData;
//	}
//
//
//	private int getNextRandom(String type, int max, double skew) {
//		if ("uniform".equals(type)) {
//			return RandUtils.nexUniformInt(max);
//		}
//		else {
//			return RandUtils.nextSkewedNormalInt(0, max, skew, 0);
//		}
//	}
//
//
//	private void writeMapToImage(String dirName, String name, Map<Integer, Integer> dataMap) {
//		writeMapToImage(dirName, name, dataMap, -1);
//	}
//
//	private void writeMapToImage(String dirName, String name, Map<Integer, Integer> dataMap, int markerSize) {
//		writeMapToImage(dirName, name, dataMap, markerSize, false);
//	}
//
//	private void writeMapToImage(String dirName, String name, Map<Integer, Integer> dataMap, int markerSize, boolean barChart) {
//		if (!barChart) {
//			writeMapToXYChart(dirName, name, dataMap, markerSize);
//		}
//		else {
//			writeMapToCategoryChart(dirName, name, dataMap, markerSize);
//		}
//	}
//
//	private void writeMapToCategoryChart(String dirName, String name, Map<Integer, Integer> dataMap, int markerSize) {
//		// Create Chart
//		CategoryChart chart = new CategoryChartBuilder().width(1500).height(1000).title("name").xAxisTitle("X").yAxisTitle("Y").build();
//
//		// Customize Chart
//		chart.getStyler().setDefaultSeriesRenderStyle(CategorySeries.CategorySeriesRenderStyle.Bar);
//		chart.getStyler().setChartTitleVisible(false);
//		chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
//		chart.getStyler().setHasAnnotations(true);
//
////		if (markerSize > 0) {
////			chart.getStyler().setMarkerSize(markerSize);
////		}
//
//		List<Map.Entry<Integer, Integer>> data = dataMap.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).collect(Collectors.toList());
//		List<Integer> xData = data.stream().map(Map.Entry::getKey).collect(Collectors.toList());
//		List<Integer> yData = data.stream().map(Map.Entry::getValue).collect(Collectors.toList());
//
//		chart.addSeries(name, xData, yData);
//
//		try {
//			BitmapEncoder.saveBitmap(chart, dirName + name + ".png", BitmapEncoder.BitmapFormat.PNG);
//		}
//		catch (IOException e) {
//			e.printStackTrace();
//			throw new RuntimeException(e);
//		}
//	}
//
//	private void writeMapToXYChart(String dirName, String name, Map<Integer, Integer> dataMap, int markerSize) {
//		// Create Chart
//		XYChart chart = new XYChartBuilder().width(1500).height(1000).title("name").xAxisTitle("X").yAxisTitle("Y").build();
//
//		// Customize Chart
//		chart.getStyler().setDefaultSeriesRenderStyle(XYSeries.XYSeriesRenderStyle.Line);
//		chart.getStyler().setChartTitleVisible(false);
//		chart.getStyler().setLegendPosition(Styler.LegendPosition.OutsideS);
//
//		if (markerSize > 0) {
//			chart.getStyler().setMarkerSize(markerSize);
//		}
//
//		List<Map.Entry<Integer, Integer>> data = dataMap.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).collect(Collectors.toList());
//		List<Integer> xData = data.stream().map(Map.Entry::getKey).collect(Collectors.toList());
//		List<Integer> yData = data.stream().map(Map.Entry::getValue).collect(Collectors.toList());
//
//		chart.addSeries(name, xData, yData);
//
//		try {
//			BitmapEncoder.saveBitmap(chart, dirName + name + ".png", BitmapEncoder.BitmapFormat.PNG);
//		}
//		catch (IOException e) {
//			e.printStackTrace();
//			throw new RuntimeException(e);
//		}
//	}
//
//	private void writeMapToFile(String file, Map<Integer, Integer> map) {
//		List<String> items = map.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey))
//			.map(x -> x.getKey() + "," + x.getValue())
//			.collect(Collectors.toList());
//
//		try {
//			Files.write(Paths.get(file), items);
//		}
//		catch (IOException e) {
//			e.printStackTrace();
//			throw new RuntimeException(e);
//		}
//
//
//	}
//
//
//	private int getRandomWordId(String wordDist, int wordSkew, int wordListSize, int vNodeId, Integer parallelism) {
//		double pV = ((double) parallelism * nVirtualNodes);
//		int bound = (int) Math.ceil((wordListSize - maxParallelism * vNodeId / pV) / (double) maxParallelism);
//		int k = getNextRandom(wordDist, bound, wordSkew);
//		return (int) Math.ceil(maxParallelism * (k + vNodeId / pV));
//	}
//
//	private int getRandomVNodeIdFromOperatorId(int opId, int parallelism, String vNodeDist, int skew) {
//		int k = getNextRandom(vNodeDist, nVirtualNodes, skew);
//		return k * parallelism + opId;
//	}
//
//	private int getVNodeIdFromWord(Object key, int parallelism) {
//		return VNodeUtils.getVNode(key, maxParallelism, nVirtualNodes, parallelism);
//	}
//
//
//}
