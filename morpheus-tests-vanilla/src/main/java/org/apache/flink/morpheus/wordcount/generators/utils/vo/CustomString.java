package org.apache.flink.morpheus.wordcount.generators.utils.vo;

import java.util.Objects;

import scala.Serializable;

public final class CustomString implements Serializable, Cloneable {
	private String value;
	private int code;
	private long eventTime;

	public CustomString(String value) {
		this.value = value;
	}

	public CustomString(String value, int code) {
		this.value = value;
		this.code = code;
	}

	public CustomString(String value, int code, long eventTime) {
		this.value = value;
		this.code = code;
		this.eventTime = eventTime;
	}

	public String getValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CustomString that = (CustomString) o;
		return Objects.equals(value, that.value);
	}

	@Override
	public int hashCode() {
		if (this.code >= 0) {
			return this.code;
		}

		int code = 0;
		if (value != null) {
			for (Character ch :
				value.toCharArray()) {
				code += ch;
			}
		}
		return code;
	}


	public static CustomString valueOf(String value) {
		return new CustomString(value);
	}

	public static CustomString copyOf(CustomString x) {
		return new CustomString(x.value, x.code, x.eventTime);
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public long getEventTime() {
		return eventTime;
	}

	public void setEventTime(long eventTime) {
		this.eventTime = eventTime;
	}
}
