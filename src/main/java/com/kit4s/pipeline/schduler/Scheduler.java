package com.kit4s.pipeline.schduler;

import com.kit4s.pipeline.Pipeline;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Scheduler {

    private final Duration defaultTimeout = Duration.ofSeconds(5);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final CountDownLatch shutdown = new CountDownLatch(1);
    private static final int WORKER_CAPACITY = 1_000;

    private final ScheduledExecutorService scheduler;

    private final ExecutorService servicesExecutor;

    private final Collection<CompletableFuture<?>> pendingFutures = new ConcurrentLinkedDeque<>();

    public CompletableFuture<Void> startPipeline(final Pipeline<?> pipeline) {
        final CompletableFuture<Void> pipelineFuture = pipeline.start(servicesExecutor);
        pendingFutures.add(pipelineFuture);
        pipelineFuture.whenComplete((r, t) -> pendingFutures.remove(pipelineFuture));
        return pipelineFuture;
    }

}
