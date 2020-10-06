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
    implements StreamObserver<GrpcResponseT> {
  private static final GrpcErrorMapper DEFAULT_ERROR_MAPPER = new GrpcErrorMapper();

  private final StreamObserver<GrpcResponseT> delegate;
  private final GrpcErrorMapper errorMapper;

  public ErrorMappingStreamObserver(final StreamObserver<GrpcResponseT> delegate) {
    this(delegate, DEFAULT_ERROR_MAPPER);
  }

  public ErrorMappingStreamObserver(
      final StreamObserver<GrpcResponseT> delegate, final GrpcErrorMapper errorMapper) {
    this.delegate = delegate;
    this.errorMapper = errorMapper;

    if (this.delegate instanceof ServerCallStreamObserver) {
      suppressCancelException();
    }
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

  private void suppressCancelException() {
    ((ServerCallStreamObserver<GrpcResponseT>) delegate).setOnCancelHandler(this::onCancel);
  }

  private void onCancel() {
    Loggers.GATEWAY_LOGGER.trace(
        "Attempted to respond to a cancelled call, indicating the client most likely went away");
  }
}
