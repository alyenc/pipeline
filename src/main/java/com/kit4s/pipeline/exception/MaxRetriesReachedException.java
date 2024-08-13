package com.kit4s.pipeline.exception;

public class MaxRetriesReachedException extends TaskException {

    public MaxRetriesReachedException() {
        super(FailureReason.MAX_RETRIES_REACHED);
    }
}
