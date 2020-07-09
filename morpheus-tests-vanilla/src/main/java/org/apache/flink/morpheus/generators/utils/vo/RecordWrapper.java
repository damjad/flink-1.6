package org.apache.flink.morpheus.generators.utils.vo;

public class RecordWrapper<T> {
	private Object key;
	private T record;
	private long ingestionTime;
	private long eventTime;

	public RecordWrapper(T record) {
		this.record = record;
	}

	public RecordWrapper(T record, long ingestionTime) {
		this.record = record;
		this.ingestionTime = ingestionTime;
	}

	public RecordWrapper(T record, long ingestionTime, long eventTime) {
		this.record = record;
		this.ingestionTime = ingestionTime;
		this.eventTime = eventTime;
	}

	public Object getKey() {
		return key;
	}

	public void setKey(Object key) {
		this.key = key;
	}

	public T getRecord() {
		return record;
	}

	public void setRecord(T record) {
		this.record = record;
	}

	public long getIngestionTime() {
		return ingestionTime;
	}

	public void setIngestionTime(long ingestionTime) {
		this.ingestionTime = ingestionTime;
	}

	public long getEventTime() {
		return eventTime;
	}

	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}
}
