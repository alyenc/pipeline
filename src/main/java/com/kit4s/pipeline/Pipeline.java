package com.kit4s.pipeline;

import com.kit4s.pipeline.exception.ExceptionUtils;
import com.kit4s.pipeline.exception.PipelineOperationException;
import com.kit4s.pipeline.pipe.Pipe;
import com.kit4s.pipeline.stage.CompleterStage;
import com.kit4s.pipeline.stage.Stage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * The Pipeline.
 */
@Slf4j
public class Pipeline<I> {

    /**
     * 输入
     */
    @Getter
    private final Pipe<I> inputPipe;

    /**
     * 中间处理步骤
     */
    private final Collection<Stage> stages;

    /**
     * 中间管道
     */
    private final Collection<Pipe<?>> pipes;

    /**
     * 结束
     */
    private final CompleterStage<?> completerStage;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private final AtomicBoolean completing = new AtomicBoolean(false);

    private volatile List<Future<?>> futures;

    private final CompletableFuture<Void> overallFuture = new CompletableFuture<>();

    public Pipeline(final Pipe<I> inputPipe,
                    final Collection<Stage> stages,
                    final Collection<Pipe<?>> pipes,
                    final CompleterStage<?> completerStage) {
        this.inputPipe = inputPipe;
        this.stages = stages;
        this.pipes = pipes;
        this.completerStage = completerStage;
    }

    /**
     * 启动执行管道
     */
    public synchronized CompletableFuture<Void> start(final ExecutorService executorService) {
        if (!started.compareAndSet(false, true)) {
            return overallFuture;
        }
        futures = Stream.concat(stages.stream(), Stream.of(completerStage))
                        .map(task -> runWithErrorHandling(executorService, task))
                        .collect(toList());
        completerStage
                .getFuture()
                .whenComplete((result, error) -> {
                    if (completing.compareAndSet(false, true)) {
                        if (error != null) {
                            overallFuture.completeExceptionally(error);
                        } else {
                            overallFuture.complete(null);
                        }
                    }
                });
        overallFuture.exceptionally(error -> {
            if (ExceptionUtils.rootCause(error) instanceof CancellationException) {
                abort();
            }
            return null;
        });
        return overallFuture;
    }

    /**
     * 取消
     */
    public void abort() {
        final CancellationException exception = new CancellationException("Pipeline aborted");
        abort(exception);
    }

    private synchronized void abort(final Throwable error) {
        if (completing.compareAndSet(false, true)) {
            inputPipe.abort();
            pipes.forEach(Pipe::abort);
            futures.forEach(future -> future.cancel(true));
            overallFuture.completeExceptionally(error);
        }
    }

    private Future<?> runWithErrorHandling(final ExecutorService executorService, final Stage task) {
        return executorService.submit(
                () -> {
                    log.info("Starting stage {}", task.getClass().getSimpleName());

                    final Thread thread = Thread.currentThread();
                    final String originalName = thread.getName();
                    try {
                        thread.setName(originalName);
                        task.run();
                    } catch (final Throwable t) {
                        if (t instanceof CompletionException
                                || t instanceof CancellationException
                                || t instanceof PipelineOperationException) {
                            log.trace("Unhandled exception in pipeline. Aborting.", t);
                        } else {
                            log.debug("Unexpected exception in pipeline. Aborting.", t);
                        }
                        try {
                            abort(t);
                        } catch (final Throwable t2) {
                            log.error("Failed to abort pipeline after error", t2);
                        }
                    } finally {
                        thread.setName(originalName);
                    }
                });
    }
}
