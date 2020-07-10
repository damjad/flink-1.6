package org.apache.flink.morpheus.wordcount.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.morpheus.wordcount.generators.utils.vo.CustomString;

import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;

public class CustomStringDeserializer implements DeserializationSchema<CustomString[]> {


	@Override
	public CustomString[] deserialize(byte[] message) throws IOException {
		return SerializationUtils.deserialize(message);
	}

	@Override
	public boolean isEndOfStream(CustomString[] nextElement) {
		return false;
	}

	@Override
	public TypeInformation<CustomString[]> getProducedType() {
		return null;
	}
}
