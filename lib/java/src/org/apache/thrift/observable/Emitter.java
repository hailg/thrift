package org.apache.thrift.observable;

public interface Emitter<T> {
	boolean emit(T data, boolean isLast);
}
