/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.thrift.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates an async method call
 * Need to generate:
 *   - private void write_args(TProtocol protocol)
 *   - public T getResult() throws <Exception_1>, <Exception_2>, ...
 * @param <T>
 */
public abstract class TAsyncMethodCall<T> {

  private static final int INITIAL_MEMORY_BUFFER_SIZE = 128;
  private static final Logger LOGGER = LoggerFactory.getLogger(TAsyncMethodCall.class.getName());

  private static AtomicLong sequenceIdCounter = new AtomicLong(0);

  private class ResultWrapper<T> {
    T result;
    int msgType;
    Exception e;

    public ResultWrapper(Exception e, int msgType) {
      this.e = e;
      this.msgType = msgType;
    }

    public ResultWrapper(T result, int msgType) {
      this.result = result;
      this.msgType = msgType;
      this.e = e;
    }
  }

  public static enum State {
    CONNECTING,
    WRITING_REQUEST_SIZE,
    WRITING_REQUEST_BODY,
    READING_RESPONSE_SIZE,
    READING_RESPONSE_BODY,
    RESPONSE_READ,
    ERROR;
  }

  /**
   * Next step in the call, initialized by start()
   */
  private State state = null;

  protected final TNonblockingTransport transport;
  private final TProtocolFactory protocolFactory;
  protected final TAsyncClient client;
  private final AsyncMethodCallback<T> callback;
  private final boolean isOneway;
  private long sequenceId;
  private final long timeout;

  private ByteBuffer sizeBuffer;
  private final byte[] sizeBufferArray = new byte[4];
  private ByteBuffer frameBuffer;

  private long startTime = System.currentTimeMillis();

  protected TAsyncMethodCall(TAsyncClient client, TProtocolFactory protocolFactory, TNonblockingTransport transport, AsyncMethodCallback<T> callback, boolean isOneway) {
    this.transport = transport;
    this.callback = callback;
    this.protocolFactory = protocolFactory;
    this.client = client;
    this.isOneway = isOneway;
    this.sequenceId = TAsyncMethodCall.sequenceIdCounter.getAndIncrement();
    this.timeout = client.getTimeout();
  }

  protected State getState() {
    return state;
  }

  protected boolean isFinished() {
    return state == State.RESPONSE_READ;
  }

  protected long getStartTime() {
    return startTime;
  }
  
  protected long getSequenceId() {
    return sequenceId;
  }

  public TAsyncClient getClient() {
    return client;
  }
  
  public boolean hasTimeout() {
    return timeout > 0;
  }
  
  public long getTimeoutTimestamp() {
    return timeout + startTime;
  }

  protected abstract void write_args(TProtocol protocol) throws TException;

  /**
   * Initialize buffers.
   * @throws TException if buffer initialization fails
   */
  protected void prepareMethodCall() throws TException {
    TMemoryBuffer memoryBuffer = new TMemoryBuffer(INITIAL_MEMORY_BUFFER_SIZE);
    TProtocol protocol = protocolFactory.getProtocol(memoryBuffer);
    write_args(protocol);

    int length = memoryBuffer.length();
    frameBuffer = ByteBuffer.wrap(memoryBuffer.getArray(), 0, length);

    TFramedTransport.encodeFrameSize(length, sizeBufferArray);
    sizeBuffer = ByteBuffer.wrap(sizeBufferArray);
  }

  /**
   * Register with selector and start first state, which could be either connecting or writing.
   * @throws IOException if register or starting fails
   */
  void start(Selector sel) throws IOException {
    SelectionKey key;
    if (transport.isOpen()) {
      state = State.WRITING_REQUEST_SIZE;
      key = transport.registerSelector(sel, SelectionKey.OP_WRITE);
    } else {
      state = State.CONNECTING;
      key = transport.registerSelector(sel, SelectionKey.OP_CONNECT);

      // non-blocking connect can complete immediately,
      // in which case we should not expect the OP_CONNECT
      if (transport.startConnect()) {
        registerForFirstWrite(key);
      }
    }

    key.attach(this);
  }

  void registerForFirstWrite(SelectionKey key) throws IOException {
    state = State.WRITING_REQUEST_SIZE;
    key.interestOps(SelectionKey.OP_WRITE);
  }

  protected ByteBuffer getFrameBuffer() {
    return frameBuffer;
  }

  /**
   * Transition to next state, doing whatever work is required. Since this
   * method is only called by the selector thread, we can make changes to our
   * select interests without worrying about concurrency.
   * @param key
   */
  protected void transition(SelectionKey key) {
    // Ensure key is valid
    if (!key.isValid()) {
      key.cancel();
      Exception e = new TTransportException("Selection key not valid!");
      onError(e);
      return;
    }

    // Transition function
    try {
      switch (state) {
        case CONNECTING:
          doConnecting(key);
          break;
        case WRITING_REQUEST_SIZE:
          doWritingRequestSize();
          break;
        case WRITING_REQUEST_BODY:
          doWritingRequestBody(key);
          break;
        case READING_RESPONSE_SIZE:
          doReadingResponseSize();
          break;
        case READING_RESPONSE_BODY:
          doReadingResponseBody(key);
          break;
        default: // RESPONSE_READ, ERROR, or bug
          throw new IllegalStateException("Method call in state " + state
              + " but selector called transition method. Seems like a bug...");
      }
    } catch (Exception e) {
      key.cancel();
      key.attach(null);
      onError(e);
    }
  }

  protected void onError(Exception e) {
    client.onError(e);
    callback.onError(e);
    state = State.ERROR;
  }

  private void doReadingResponseBody(SelectionKey key) throws IOException {
    if (transport.read(frameBuffer) < 0) {
      throw new IOException("Read call frame failed");
    }
    if (frameBuffer.remaining() == 0) {
      cleanUpAndFireCallback(key);
    }
  }

  private void cleanUpAndFireCallback(SelectionKey key) {
    state = State.RESPONSE_READ;
    try {
      ResultWrapper<T> resultWrapper = receive();
      switch (resultWrapper.msgType) {
        case TMessageType.EXCEPTION:
          notifyException(key, resultWrapper);
          break;
        case TMessageType.REPLY:
          notifyCallReply(key, resultWrapper);
          break;
        case TMessageType.OBSERVABLE:
          notifyObservable(key, resultWrapper);
          break;
        case TMessageType.OBSERVABLE_ENDED:
          notifyObservableEnd(key, resultWrapper);
          break;
      }
    } catch (TException e) {
      //This will never happen
      LOGGER.error("cleanUpAndFireCallback", e);
    }
  }

  private void notifyException(SelectionKey key, ResultWrapper<T> resultWrapper) {
    key.interestOps(0);
    // this ensures that the TAsyncMethod instance doesn't hang around
    key.attach(null);
    client.onComplete();
    try {
      callback.onError(resultWrapper.e);
    } catch (Exception e) {
      LOGGER.error("notifyException", e);
    }
  }

  private void notifyCallReply(SelectionKey key, ResultWrapper<T> resultWrapper) {
    key.interestOps(0);
    // this ensures that the TAsyncMethod instance doesn't hang around
    key.attach(null);
    client.onComplete();

    try {
      if (resultWrapper.e != null) {
        callback.onError(resultWrapper.e);
      } else {
        callback.onComplete(resultWrapper.result);
      }
    } catch (Exception e) {
      LOGGER.error("Callback Exception", e);
    }
  }

  private void notifyObservable(SelectionKey key, ResultWrapper<T> resultWrapper) {
    try {
      if (resultWrapper.e != null) {
        callback.onError(resultWrapper.e);
      } else {
        callback.onProcess(resultWrapper.result);
      }
    } catch (Exception e) {
      LOGGER.error("Callback Exception", e);
    }

    //We're expecting more messages
    state = State.READING_RESPONSE_SIZE;
    sizeBuffer.rewind();  // Prepare to read incoming frame size
    key.interestOps(SelectionKey.OP_READ);
  }

  private void notifyObservableEnd(SelectionKey key, ResultWrapper<T> resultWrapper) {
    key.interestOps(0);
    // this ensures that the TAsyncMethod instance doesn't hang around
    key.attach(null);
    client.onComplete();

    try {
      if (resultWrapper.e != null) {
        callback.onError(resultWrapper.e);
      } else {
        callback.onComplete(resultWrapper.result);
      }
    } catch (Exception e) {
      LOGGER.error("Callback Exception", e);
    }
  }

  private void doReadingResponseSize() throws IOException {
    if (transport.read(sizeBuffer) < 0) {
      throw new IOException("Read call frame size failed");
    }
    if (sizeBuffer.remaining() == 0) {
      state = State.READING_RESPONSE_BODY;
      frameBuffer = ByteBuffer.allocate(TFramedTransport.decodeFrameSize(sizeBufferArray));
    }
  }

  private void doWritingRequestBody(SelectionKey key) throws IOException {
    if (transport.write(frameBuffer) < 0) {
      throw new IOException("Write call frame failed");
    }
    if (frameBuffer.remaining() == 0) {
      if (isOneway) {
        cleanUpAndFireCallback(key);
      } else {
        state = State.READING_RESPONSE_SIZE;
        sizeBuffer.rewind();  // Prepare to read incoming frame size
        key.interestOps(SelectionKey.OP_READ);
      }
    }
  }

  private void doWritingRequestSize() throws IOException {
    if (transport.write(sizeBuffer) < 0) {
      throw new IOException("Write call frame size failed");
    }
    if (sizeBuffer.remaining() == 0) {
      state = State.WRITING_REQUEST_BODY;
    }
  }

  private void doConnecting(SelectionKey key) throws IOException {
    if (!key.isConnectable() || !transport.finishConnect()) {
      throw new IOException("not connectable or finishConnect returned false after we got an OP_CONNECT");
    }
    registerForFirstWrite(key);
  }

  protected abstract T getResult(TProtocol protocol) throws org.apache.thrift.TException ;
  protected abstract TProtocol getInputProtocol();

  private ResultWrapper<T> receive() throws TException {
    TProtocol protocol = getInputProtocol();
    TMessage msg = protocol.readMessageBegin();
    if (msg.type == TMessageType.EXCEPTION) {
      TApplicationException x = TApplicationException.read(protocol);
      protocol.readMessageEnd();
      return new ResultWrapper<>(x, msg.type);
    }
    try {
      T result = getResult(protocol);
      return new ResultWrapper(result, msg.type);
    } catch (Exception e) {
      return new ResultWrapper<>(e, msg.type);
    } finally {
      protocol.readMessageEnd();
    }
  }
}
