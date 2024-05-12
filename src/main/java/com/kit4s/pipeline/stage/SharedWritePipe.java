package com.kit4s.pipeline.stage;

import com.kit4s.pipeline.pipe.WritePipe;

import java.util.concurrent.atomic.AtomicInteger;

public class SharedWritePipe<T> implements WritePipe<T> {

    private final WritePipe<T> delegate;

    private final AtomicInteger remainingClosesRequired;

    public SharedWritePipe(final WritePipe<T> delegate, final int closesRequired) {
        this.delegate = delegate;
        this.remainingClosesRequired = new AtomicInteger(closesRequired);
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public void put(final T value) {
        delegate.put(value);
    }

    @Override
    public void close() {
        if (remainingClosesRequired.decrementAndGet() == 0) {
            delegate.close();
        }
    }

    @Override
    public void abort() {
        delegate.abort();
    }

    @Override
    public boolean hasRemainingCapacity() {
        return delegate.hasRemainingCapacity();
    }
}
