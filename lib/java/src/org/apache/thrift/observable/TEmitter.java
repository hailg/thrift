package org.apache.thrift.observable;

import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TEmitter<V> implements Emitter<V> {
	private static final Logger LOGGER = LoggerFactory.getLogger(TEmitter.class.getName());
	
	private final ObservableProcessFunction<?, V, ?, ?> observableFunction;
	
	TEmitter(ObservableProcessFunction<?, V, ?, ?> observableFunction) {
		this.observableFunction = observableFunction;
	}
	
	@Override
	public boolean emit(V data, boolean isLast) {
		try {
			observableFunction.send(data, isLast);
			return true;
		} catch (Exception e) {
			LOGGER.error("Cannot emit data", e);
		}
		return false;
	}
}
