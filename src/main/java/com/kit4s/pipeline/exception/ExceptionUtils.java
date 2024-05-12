package com.kit4s.pipeline.exception;

import com.google.common.base.Throwables;

public class ExceptionUtils {

    private ExceptionUtils() {}

    public static Throwable rootCause(final Throwable throwable) {
        return throwable != null ? Throwables.getRootCause(throwable) : null;
    }
}
