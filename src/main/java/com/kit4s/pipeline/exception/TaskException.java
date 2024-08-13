package com.kit4s.pipeline.exception;

public class TaskException extends RuntimeException {

    private final FailureReason failureReason;

    TaskException(final FailureReason failureReason) {
        this("Task failed: " + failureReason.name(), failureReason);
    }

    TaskException(final String message, final FailureReason failureReason) {
        super(message);
        this.failureReason = failureReason;
    }

    public FailureReason reason() {
        return failureReason;
    }

    public enum FailureReason {
        MAX_RETRIES_REACHED
    }
}
