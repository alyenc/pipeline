package com.kit4s.pipeline.pipe;

import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class Pipe<T> implements ReadPipe<T>, WritePipe<T> {

    private final BlockingQueue<T> queue;

    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicBoolean aborted = new AtomicBoolean();

    public Pipe(final int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    @Override
    public boolean hasMore() {
        if (aborted.get()) {
            return false;
        }
        return !closed.get() || !queue.isEmpty();
    }

    @Override
    public boolean isAborted() {
        return aborted.get();
    }

    @Override
    public int drainTo(Collection<T> output, int maxElements) {
        return 0;
    }

    @Override
    public T get() {
        try {
            while (hasMore()) {
                final T value = queue.poll(1, TimeUnit.SECONDS);
                if (value != null) {
                    return value;
                }
            }
        } catch (final InterruptedException e) {
            log.trace("Interrupted while waiting for next item", e);
        }
        return null;
    }

    @Override
    public T poll() {
        return queue.poll();
    }

    @Override
    public boolean isOpen() {
        return !closed.get() && !aborted.get();
    }

    @Override
    public void put(T value) {
        while (isOpen()) {
            try {
                if (queue.offer(value, 1, TimeUnit.SECONDS)) {
                    return;
                }
            } catch (final InterruptedException e) {
                log.trace("Interrupted while waiting to add to output", e);
            }
        }
    }

    @Override
    public boolean hasRemainingCapacity() {
        return queue.remainingCapacity() > 0 && isOpen();
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public void abort() {
        aborted.compareAndSet(false, true);
    }
}
