/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.grpc;

import com.google.rpc.Code;
import com.google.rpc.Status;
import com.google.rpc.Status.Builder;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.zeebe.gateway.Loggers;
import io.zeebe.gateway.cmd.BrokerErrorException;
import io.zeebe.gateway.cmd.BrokerRejectionException;
import io.zeebe.gateway.cmd.InvalidBrokerRequestArgumentException;
import io.zeebe.gateway.cmd.PartitionNotFoundException;
import io.zeebe.gateway.impl.broker.response.BrokerError;
import io.zeebe.gateway.impl.broker.response.BrokerRejection;
import io.zeebe.msgpack.MsgpackPropertyException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;

/** Maps arbitrary {@link Throwable} to {@link StatusRuntimeException} and logs the exception. */
public final class GrpcErrorMapper {
  private final Logger logger;

  public GrpcErrorMapper() {
    this(Loggers.GATEWAY_LOGGER);
  }

  public GrpcErrorMapper(final Logger logger) {
    this.logger = logger;
  }

  public StatusRuntimeException mapError(final Throwable error) {
    return StatusProto.toStatusRuntimeException(mapErrorToStatus(error));
  }

  private Status mapErrorToStatus(final Throwable error) {
    final Builder builder = Status.newBuilder();

    if (error instanceof ExecutionException) {
      return mapErrorToStatus(error.getCause());
    } else if (error instanceof BrokerErrorException) {
      final Status status = mapBrokerErrorToStatus(((BrokerErrorException) error).getError());
      builder.mergeFrom(status);

      // When there is back pressure, there will be a lot of `RESOURCE_EXHAUSTED` errors and the log
      // can get flooded, so log them at the lowest level possible
      if (status.getCode() != Code.RESOURCE_EXHAUSTED.getNumber()) {
        logger.error("Expected to handle gRPC request, but received error from broker", error);
      } else {
        logger.trace("Expected to handle gRPC request, but broker is overloaded", error);
      }
    } else if (error instanceof BrokerRejectionException) {
      final Status status = mapRejectionToStatus(((BrokerRejectionException) error).getRejection());
      builder.mergeFrom(status);

      logger.debug("Expected to handle gRPC request, but broker rejected request", error);
    } else if (error instanceof TimeoutException) { // can be thrown by transport
      builder
          .setCode(Code.DEADLINE_EXCEEDED_VALUE)
          .setMessage("Time out between gateway and broker: " + error.getMessage());
      logger.debug(
          "Expected to handle gRPC request, but request timed out between gateway and broker",
          error);
    } else if (error instanceof InvalidBrokerRequestArgumentException) {
      builder.setCode(Code.INVALID_ARGUMENT_VALUE).setMessage(error.getMessage());
      logger.debug("Expected to handle gRPC request, but broker argument was invalid", error);
    } else if (error instanceof MsgpackPropertyException) {
      builder.setCode(Code.INVALID_ARGUMENT_VALUE).setMessage(error.getMessage());
      logger.debug("Expected to handle gRPC request, but messagepack property was invalid", error);
    } else if (error instanceof PartitionNotFoundException) {
      builder.setCode(Code.NOT_FOUND_VALUE).setMessage(error.getMessage());
      logger.debug("Expected to handle gRPC request, but request could not be delivered", error);
    } else {
      builder
          .setCode(Code.INTERNAL_VALUE)
          .setMessage(
              "Unexpected error occurred during the request processing: " + error.getMessage());
      logger.error("Expected to handle gRPC request, but an unexpected error occurred", error);
    }

    return builder.build();
  }

  private Status mapBrokerErrorToStatus(final BrokerError error) {
    final Builder builder = Status.newBuilder();
    String message = error.getMessage();

    switch (error.getCode()) {
      case WORKFLOW_NOT_FOUND:
        builder.setCode(Code.NOT_FOUND_VALUE);
        break;
      case RESOURCE_EXHAUSTED:
        builder.setCode(Code.RESOURCE_EXHAUSTED_VALUE);
        break;
      default:
        builder.setCode(Code.INTERNAL_VALUE);
        message =
            String.format(
                "Unexpected error occurred between gateway and broker (code: %s)", error.getCode());
        break;
    }

    return builder.setMessage(message).build();
  }

  private Status mapRejectionToStatus(final BrokerRejection rejection) {
    final String message =
        String.format(
            "Command rejected with code '%s': %s", rejection.getIntent(), rejection.getReason());
    final Code code;

    switch (rejection.getType()) {
      case INVALID_ARGUMENT:
        code = Code.INVALID_ARGUMENT;
        break;
      case NOT_FOUND:
        code = Code.NOT_FOUND;
        break;
      case ALREADY_EXISTS:
        code = Code.ALREADY_EXISTS;
        break;
      case INVALID_STATE:
        code = Code.FAILED_PRECONDITION;
        break;
      default:
        code = Code.UNKNOWN;
        break;
    }

    return Status.newBuilder().setMessage(message).setCode(code.getNumber()).build();
  }
}
