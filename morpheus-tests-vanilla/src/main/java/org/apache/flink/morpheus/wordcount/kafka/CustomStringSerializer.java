package org.apache.flink.morpheus.wordcount.kafka;

import org.apache.flink.morpheus.wordcount.generators.utils.vo.CustomString;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CustomStringSerializer implements Serializer<CustomString[]> {


	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public byte[] serialize(String topic, CustomString[] data) {
		return SerializationUtils.serialize(data);
	}

	@Override
	public void close() {

	}
}
