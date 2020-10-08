/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.grpc;

import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.zeebe.gateway.Loggers;

/**
 * A {@link StreamObserver} decorator which will map errors to {@link StatusException} before
 * passing them on to the {@code delegate}.
 *
 * <p>Additionally, if the decorated {@link StreamObserver} is a {@link ServerCallStreamObserver},
 * it will install a cancellation handler to effectively suppress the {@code delegate} from throwing
 * an exception if the call was already cancelled.
 *
 * @param <GrpcResponseT>
 */
@SuppressWarnings("java:S119")
public final class ErrorMappingStreamObserver<GrpcResponseT>
    extends ServerCallStreamObserver<GrpcResponseT> {
  private static final GrpcErrorMapper DEFAULT_ERROR_MAPPER = new GrpcErrorMapper();

  private final ServerCallStreamObserver<GrpcResponseT> delegate;
  private final GrpcErrorMapper errorMapper;

  public ErrorMappingStreamObserver(final ServerCallStreamObserver<GrpcResponseT> delegate) {
    this(delegate, DEFAULT_ERROR_MAPPER);
  }

  public ErrorMappingStreamObserver(
      final ServerCallStreamObserver<GrpcResponseT> delegate, final GrpcErrorMapper errorMapper) {
    this.delegate = delegate;
    this.errorMapper = errorMapper;

    suppressCancelException();
  }

  public static <GrpcResponseT> ErrorMappingStreamObserver<GrpcResponseT> ofStreamObserver(
      final StreamObserver<GrpcResponseT> streamObserver) {
    if (!(streamObserver instanceof ServerCallStreamObserver)) {
      throw new IllegalArgumentException(
          String.format(
              "Expected to wrap a server call stream observer, but got %s; this class can only be used server-side",
              streamObserver.getClass()));
    }

    return new ErrorMappingStreamObserver<>(
        (ServerCallStreamObserver<GrpcResponseT>) streamObserver);
  }

  @Override
  public boolean isReady() {
    return delegate.isReady();
  }

  @Override
  public void setOnReadyHandler(final Runnable onReadyHandler) {
    delegate.setOnReadyHandler(onReadyHandler);
  }

  @Override
  public void disableAutoInboundFlowControl() {
    delegate.disableAutoInboundFlowControl();
  }

  @Override
  public void request(final int count) {
    delegate.request(count);
  }

  @Override
  public void setMessageCompression(final boolean enable) {
    delegate.setMessageCompression(enable);
  }

  @Override
  public void onNext(final GrpcResponseT value) {
    delegate.onNext(value);
  }

  @Override
  public void onError(final Throwable t) {
    final Throwable mapped;

    if (t instanceof StatusException || t instanceof StatusRuntimeException) {
      mapped = t;
    } else {
      mapped = errorMapper.mapError(t);
    }

    delegate.onError(mapped);
  }

  @Override
  public void onCompleted() {
    delegate.onCompleted();
  }

  @Override
  public boolean isCancelled() {
    return delegate.isCancelled();
  }

  @Override
  public void setOnCancelHandler(final Runnable onCancelHandler) {
    delegate.setOnCancelHandler(onCancelHandler);
  }

  @Override
  public void setCompression(final String compression) {
    delegate.setCompression(compression);
  }

  private void suppressCancelException() {
    delegate.setOnCancelHandler(this::onCancel);
  }

  private void onCancel() {
    Loggers.GATEWAY_LOGGER.trace(
        "Attempted to respond to a cancelled call, indicating the client most likely went away");
  }
}
