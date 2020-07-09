package org.apache.flink.metrics;

public class LongContainerGauge implements Gauge<Long> {

	private long value;

	public void addValue(long value) {
		this.value += value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	@Override
	public Long getValue() {
		return value;
	}
}
