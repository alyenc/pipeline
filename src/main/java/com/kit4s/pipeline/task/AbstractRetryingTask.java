package com.kit4s.pipeline.task;

import com.kit4s.pipeline.exception.ExceptionUtils;
import com.kit4s.pipeline.exception.MaxRetriesReachedException;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

@Slf4j
public abstract class AbstractRetryingTask<T> extends AbstractTask<T> {

    private int retryCount = 0;

    private final int maxRetries;

    private final Predicate<T> isEmptyResponse;

    protected AbstractRetryingTask(
            final int maxRetries,
            final Predicate<T> isEmptyResponse) {
        this.maxRetries = maxRetries;
        this.isEmptyResponse = isEmptyResponse;
    }

    @Override
    protected void executeTask() {
        if (result.isDone()) {
            // Return if task is done
            return;
        }
        if (retryCount >= maxRetries) {
            result.completeExceptionally(new MaxRetriesReachedException());
            return;
        }

        retryCount += 1;
        executeRetryingTask()
                .whenComplete((peerResult, error) -> {
                    if (error != null) {
                        handleTaskError(error);
                    } else {
                        if (!isEmptyResponse.test(peerResult)) {
                            retryCount = 0;
                        }
                        executeTask();
                    }
                });

    }

    protected void handleTaskError(final Throwable error) {
        final Throwable cause = ExceptionUtils.rootCause(error);
        if (!isRetryableError(cause)) {
            result.completeExceptionally(cause);
            return;
        }
    }

    protected boolean isRetryableError(final Throwable error) {
        return error instanceof TimeoutException;
    }

    protected abstract CompletableFuture<T> executeRetryingTask();
}
