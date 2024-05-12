package com.kit4s.pipeline.pipe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class BatchReadPipe<T> implements ReadPipe<List<T>> {

    private final ReadPipe<T> input;

    private final int maximumBatchSize;

    private final Function<List<T>, Integer> stopBatchCondition;

    public BatchReadPipe(
            final ReadPipe<T> input, final int maximumBatchSize) {
        this(input, maximumBatchSize, ts -> maximumBatchSize - ts.size());
    }

    public BatchReadPipe(
            final ReadPipe<T> input,
            final int maximumBatchSize,
            final Function<List<T>, Integer> batchEndCondition) {
        this.input = input;
        this.maximumBatchSize = maximumBatchSize;
        this.stopBatchCondition = batchEndCondition;
    }

    @Override
    public boolean hasMore() {
        return input.hasMore();
    }

    @Override
    public boolean isAborted() {
        return input.isAborted();
    }

    @Override
    public List<T> get() {
        final T firstItem = input.get();
        if (firstItem == null) {
            return null;
        }
        final List<T> batch = new ArrayList<>();
        batch.add(firstItem);
        Integer remainingData = stopBatchCondition.apply(batch);
        while (remainingData > 0
                && (batch.size() + remainingData) <= maximumBatchSize
                && input.hasMore()) {
            if (input.drainTo(batch, remainingData) == 0) {
                break;
            }
            remainingData = stopBatchCondition.apply(batch);
        }
        return batch;
    }

    @Override
    public List<T> poll() {
        final List<T> batch = new ArrayList<>();
        Integer remainingData = stopBatchCondition.apply(batch);
        while (remainingData > 0
                && (batch.size() + remainingData) <= maximumBatchSize
                && input.hasMore()) {
            if (input.drainTo(batch, remainingData) == 0) {
                break;
            }
            remainingData = stopBatchCondition.apply(batch);
        }
        if (batch.isEmpty()) {
            // Poll has to return null if the pipe is empty
            return null;
        }
        return batch;
    }

    @Override
    public int drainTo(final Collection<List<T>> output, final int maxElements) {
        final List<T> nextBatch = poll();
        if (nextBatch != null) {
            output.add(nextBatch);
            return nextBatch.size();
        }
        return 0;
    }
}

