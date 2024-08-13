package com.kit4s.pipeline.task;

import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public abstract class AbstractTask<T> implements Task<T> {

    protected final CompletableFuture<T> result = new CompletableFuture<>();

    private final AtomicBoolean started = new AtomicBoolean(false);

    private final Collection<CompletableFuture<?>> subTaskFutures = new ConcurrentLinkedDeque<>();

    @Override
    public final CompletableFuture<T> run() {
        if (!result.isDone() && started.compareAndSet(false, true)) {
            executeTask();
            result.whenComplete((r, t) -> cleanup());
        }
        return result;
    }

    @Override
    public final CompletableFuture<T> runAsync(final ExecutorService executor) {
        if (!result.isDone() && started.compareAndSet(false, true)) {
            executor.execute(this::executeTask);
            result.whenComplete((r, t) -> cleanup());
        }
        return result;
    }

    @Override
    public final void cancel() {
        result.cancel(false);
    }

    public final boolean isDone() {
        return result.isDone();
    }

    public final boolean isFailed() {
        return result.isCompletedExceptionally();
    }

    public final boolean isCancelled() {
        return result.isCancelled();
    }

    protected final <S> CompletableFuture<S> executeSubTask(
            final Supplier<CompletableFuture<S>> subTask) {
        synchronized (result) {
            if (!isCancelled()) {
                final CompletableFuture<S> subTaskFuture = subTask.get();
                subTaskFutures.add(subTaskFuture);
                subTaskFuture.whenComplete((r, t) -> subTaskFutures.remove(subTaskFuture));
                return subTaskFuture;
            } else {
                return CompletableFuture.failedFuture(new CancellationException());
            }
        }
    }

    protected abstract void executeTask();

    protected void cleanup() {
        for (final CompletableFuture<?> subTaskFuture : subTaskFutures) {
            subTaskFuture.cancel(false);
        }
    }
}

