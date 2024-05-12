package com.kit4s.pipeline.pipe;

public interface WritePipe<T> {

    boolean isOpen();

    void put(T value);

    boolean hasRemainingCapacity();

    void close();

    void abort();
}
