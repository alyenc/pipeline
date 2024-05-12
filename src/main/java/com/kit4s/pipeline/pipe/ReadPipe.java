package com.kit4s.pipeline.pipe;

import java.util.Collection;

/**
 *
 */
public interface ReadPipe<T> {

    boolean hasMore();

    boolean isAborted();

    int drainTo(Collection<T> output, int maxElements);

    T get();

    T poll();

}
