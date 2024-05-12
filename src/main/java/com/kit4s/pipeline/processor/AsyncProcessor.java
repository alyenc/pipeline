package com.kit4s.pipeline.processor;

import com.kit4s.pipeline.exception.PipelineOperationException;
import com.kit4s.pipeline.pipe.ReadPipe;
import com.kit4s.pipeline.pipe.WritePipe;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static java.util.concurrent.CompletableFuture.completedFuture;

@Slf4j
public class AsyncProcessor<I, O> implements Processor<I, O> {

    private final Function<I, CompletableFuture<O>> processor;

    private final List<CompletableFuture<O>> inProgress;

    private final boolean preserveOrder;

    private final int maxConcurrency;

    private CompletableFuture<?> nextOutputAvailableFuture = completedFuture(null);

    public AsyncProcessor(
            final Function<I, CompletableFuture<O>> processor,
            final int maxConcurrency,
            final boolean preserveOrder) {
        this.processor = processor;
        this.inProgress = new ArrayList<>(maxConcurrency);
        this.preserveOrder = preserveOrder;
        this.maxConcurrency = maxConcurrency;
    }

    @Override
    public void process(ReadPipe<I> inputPipe, WritePipe<O> outputPipe) {
        if (inProgress.size() < maxConcurrency) {
            final I value = inputPipe.get();
            if (value != null) {
                final CompletableFuture<O> future = processor.apply(value);
                final Thread stageThread = Thread.currentThread();
                inProgress.add(future);
                updateNextOutputAvailableFuture();
                future.whenComplete((result, error) -> stageThread.interrupt());
            }
            outputCompletedTasks(outputPipe);
        } else {
            outputNextCompletedTask(outputPipe);
        }
    }

    @Override
    public boolean attemptFinalization(final WritePipe<O> outputPipe) {
        outputNextCompletedTask(outputPipe);
        return inProgress.isEmpty();
    }

    @Override
    public void abort() {
        inProgress.forEach(future -> future.cancel(true));
    }

    private void outputNextCompletedTask(final WritePipe<O> outputPipe) {
        try {
            waitForAnyFutureToComplete();
            outputCompletedTasks(outputPipe);
        } catch (final InterruptedException e) {
            log.trace("Interrupted while waiting for processing to complete", e);
        } catch (final ExecutionException e) {
            throw new PipelineOperationException("Async operation failed. " + e.getMessage(), e);
        } catch (final TimeoutException e) {
            // Ignore and go back around the loop.
        }
    }

    private void waitForAnyFutureToComplete()
            throws InterruptedException, ExecutionException, TimeoutException {
        nextOutputAvailableFuture.get(1, TimeUnit.SECONDS);
    }

    private void outputCompletedTasks(final WritePipe<O> outputPipe) {
        boolean inProgressChanged = false;
        for (final Iterator<CompletableFuture<O>> i = inProgress.iterator(); i.hasNext(); ) {
            final CompletableFuture<O> process = i.next();
            final O result = process.getNow(null);
            if (result != null) {
                inProgressChanged = true;
                outputPipe.put(result);
                i.remove();
            } else if (preserveOrder) {
                break;
            }
        }
        if (inProgressChanged) {
            updateNextOutputAvailableFuture();
        }
    }

    @SuppressWarnings("rawtypes")
    private void updateNextOutputAvailableFuture() {
        if (preserveOrder) {
            nextOutputAvailableFuture = inProgress.isEmpty() ? completedFuture(null) : inProgress.getFirst();
        } else {
            nextOutputAvailableFuture = CompletableFuture.anyOf(inProgress.toArray(new CompletableFuture[0]));
        }
    }
}
