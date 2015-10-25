package org.apache.thrift.observable;

import org.apache.thrift.ProcessFunction;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instance of this class will be map to a service's observable function. An observable function will return 
 * many messages instead of only 1 result as normal. The service cannot use this class directly, instead 
 * each service's observable function will have an Emitter.
 * 
 * @author hailegia
 *
 * @param <I> Iface
 * @param <V> Value type to emit (String, int, struct...)
 * @param <ArgsT> Argument TBase (e.g.. hello_args)
 * @param <OutT> Output TBase type (e.g., hello_result)
 */
public abstract class ObservableProcessFunction<I, V, ArgsT extends TBase, OutT extends TBase> extends ProcessFunction<I, ArgsT> {

	private static final Logger LOGGER = LoggerFactory.getLogger(ObservableProcessFunction.class.getName());
	private TProtocol oprot;
	private int seqid;

	public ObservableProcessFunction(String methodName) {
		super(methodName);
	}

	/**
	 * This will send back the data
	 * 
	 * @param data
	 * @param last
	 * @throws TException
	 */
	protected void send(V data, boolean last) throws TException {
		TBase tData = produce(data);
		oprot.writeMessageBegin(new TMessage(getMethodName(),
											last ? TMessageType.OBSERVABLE_ENDED : TMessageType.OBSERVABLE, seqid));
		tData.write(oprot);
		oprot.writeMessageEnd();
		oprot.getTransport().flush();
		if (last) {
			oprot = null;
		}
	}

	public Emitter<V> getEmitter() {
		return new TEmitter(this);
	}

	@Override
	public void process(int seqid, TProtocol iprot, TProtocol oprot, I iface) throws TException {
		this.seqid = seqid;
		this.oprot = oprot;
		ArgsT args = getEmptyArgsInstance();
		try {
			args.read(iprot);
		} catch (TProtocolException e) {
			iprot.readMessageEnd();
			TApplicationException x = new TApplicationException(TApplicationException.PROTOCOL_ERROR, e.getMessage());
			oprot.writeMessageBegin(new TMessage(getMethodName(), TMessageType.EXCEPTION, seqid));
			x.write(oprot);
			oprot.writeMessageEnd();
			oprot.getTransport().flush();
			return;
		}
		iprot.readMessageEnd();

		try {
			//We are not interested in the result anyway, just want to call it.
			getResult(iface, args);
		} catch (TException tex) {
			LOGGER.error("Internal error processing " + getMethodName(), tex);
			TApplicationException x = new TApplicationException(TApplicationException.INTERNAL_ERROR,
					"Internal error processing " + getMethodName());
			oprot.writeMessageBegin(new TMessage(getMethodName(), TMessageType.EXCEPTION, seqid));
			x.write(oprot);
			oprot.writeMessageEnd();
			oprot.getTransport().flush();
			return;
		}

	}

	protected abstract OutT produce(V data);
}
