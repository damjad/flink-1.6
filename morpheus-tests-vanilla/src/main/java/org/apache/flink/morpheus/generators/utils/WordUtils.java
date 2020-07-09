package org.apache.flink.morpheus.generators.utils;

import org.apache.flink.morpheus.generators.utils.vo.CustomString;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WordUtils {

	private static final String LETTERS = "abcdefghijklmnopqrstuvwxyz";

	public static List<String> genWords(int maxL) {
		return genWords(new ArrayList<>(), 1, maxL).stream().map(x -> x.append(" ")).map(StringBuilder::toString).collect(Collectors.toList());
	}

	public static List<StringBuilder> genWords(List<StringBuilder> builders, int l, int maxL) {
		if (l > maxL) {
			return builders;
		}
		List<StringBuilder> nBuilders = new ArrayList<>();

		if (builders == null || builders.isEmpty()) {
			for (Character c : LETTERS.toCharArray()) {
				StringBuilder b = new StringBuilder(1);
				b.append(c);
				nBuilders.add(b);
			}

			return genWords(nBuilders, l + 1, maxL);
		}

		for (StringBuilder builder : builders) {
			for (Character c : LETTERS.toCharArray()) {
				StringBuilder bd = new StringBuilder(builder);
				bd.append(c);
				nBuilders.add(bd);
			}
		}

		return genWords(nBuilders, l + 1, maxL);
	}

	public static List<CustomString> genCustomStringWords(int maxL) {
		List<String> words = genWords(maxL);
		return IntStream.range(0, words.size())
			.mapToObj(x -> new CustomString(words.get(x), x))
			.collect(Collectors.toList());
	}
}
