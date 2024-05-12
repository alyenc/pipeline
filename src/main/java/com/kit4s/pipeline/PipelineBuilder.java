package com.kit4s.pipeline;

import com.kit4s.pipeline.pipe.BatchReadPipe;
import com.kit4s.pipeline.pipe.Pipe;
import com.kit4s.pipeline.pipe.ReadPipe;
import com.kit4s.pipeline.pipe.WritePipe;
import com.kit4s.pipeline.processor.AsyncProcessor;
import com.kit4s.pipeline.processor.FlatMapProcessor;
import com.kit4s.pipeline.processor.MapProcessor;
import com.kit4s.pipeline.processor.Processor;
import com.kit4s.pipeline.stage.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

public class PipelineBuilder<I, T> {

    private final Pipe<I> inputPipe;
    /**
     * pipeline执行阶段
     */
    private final Collection<Stage> stages;

    private final Collection<Pipe<?>> pipes;

    private final ReadPipe<T> lastPipe;

    private final int bufferSize;

    private final int numberOfThreads;

    /**
     * 构造函数
     *
     * @param stages the stages
     */
    public PipelineBuilder(
            final Pipe<I> inputPipe,
            final Collection<Stage> stages,
            final Collection<Pipe<?>> pipes,
            final ReadPipe<T> lastPipe,
            final int bufferSize,
            final int numberOfThreads) {
        this.inputPipe = inputPipe;
        this.stages = stages;
        this.pipes = pipes;
        this.lastPipe = lastPipe;
        this.bufferSize = bufferSize;
        this.numberOfThreads = numberOfThreads;
    }

    public static <T> PipelineBuilder<T, T> createPipeline(
            final Iterator<T> source, final int bufferSize) {
        final Pipe<T> pipe = new Pipe<>(bufferSize);
        final IteratorStage<T> sourceStage = new IteratorStage<>(source, pipe);
        return new PipelineBuilder<>(pipe, singleton(sourceStage),
                singleton(pipe), pipe, bufferSize, 10);
    }

    /**
     * 创建一个pipeline
     */
    public static <T> PipelineBuilder<T, T> createPipeline(final int bufferSize) {
        final Pipe<T> pipe = new Pipe<>(bufferSize);
        return new PipelineBuilder<>(pipe, emptyList(), singleton(pipe), pipe,
                bufferSize, 10);
    }

    /**
     *
     */
    public <O> PipelineBuilder<I, O> thenProcess(final Function<T, O> function) {
        final Processor<T, O> singleStepStage = new MapProcessor<>(function);
        return addStage(singleStepStage);
    }

    /**
     *
     */
    public <O> PipelineBuilder<I, O> thenProcessInParallel(
            final Function<T, O> function) {
        return addParallelStage(() -> new MapProcessor<>(function), numberOfThreads, bufferSize);
    }

    /**
     *
     */
    public <O> PipelineBuilder<I, O> thenProcessInParallel(
            final Function<T, O> function,
            final int numberOfThreads) {
        return addParallelStage(() -> new MapProcessor<>(function), numberOfThreads, bufferSize);
    }

    /**
     *
     */
    public <O> PipelineBuilder<I, O> thenProcessAsync(
            final Function<T, CompletableFuture<O>> processor,
            final int maxConcurrency) {
        return addStage(new AsyncProcessor<>(processor, maxConcurrency, false));
    }

    public <O> PipelineBuilder<I, O> thenFlatMap(
            final String stageName,
            final Function<T, Stream<O>> mapper,
            final int newBufferSize) {
        return addStage(new FlatMapProcessor<>(mapper), newBufferSize);
    }

    public <O> PipelineBuilder<I, O> thenFlatMapInParallel(
            final Function<T, Stream<O>> mapper,
            final int numberOfThreads,
            final int newBufferSize) {
        return addParallelStage(() -> new FlatMapProcessor<>(mapper), numberOfThreads, newBufferSize);
    }

    public PipelineBuilder<I, List<T>> inBatches(
            final int maximumBatchSize) {
        checkArgument(maximumBatchSize > 0, "Maximum batch size must be greater than 0");
        return new PipelineBuilder<>(inputPipe, stages, pipes,
                new BatchReadPipe<>(lastPipe, maximumBatchSize),
                (int) Math.ceil(((double) bufferSize) / maximumBatchSize), numberOfThreads);
    }

    public PipelineBuilder<I, List<T>> inBatches(
            final int maximumBatchSize, final Function<List<T>, Integer> stopBatchCondition) {
        return new PipelineBuilder<>(inputPipe, stages, pipes,
                new BatchReadPipe<>(lastPipe, maximumBatchSize, stopBatchCondition),
                (int) Math.ceil(((double) bufferSize) / maximumBatchSize), numberOfThreads);
    }

    /**
     * Pipeline的结束阶段
     *
     * @param completer 结束阶段的处理器
     * @return 一个完整的Pipeline
     */
    public Pipeline<I> andFinishWith(final Consumer<T> completer) {
        CompleterStage<T> completerStage = new CompleterStage<>(lastPipe, completer);
        return new Pipeline<>(inputPipe, stages, pipes, completerStage);
    }

    private <O> PipelineBuilder<I, O> addStage(
            final Processor<T, O> processor) {
        return addStage(processor, numberOfThreads, bufferSize);
    }

    private <O> PipelineBuilder<I, O> addStage(
            final Processor<T, O> processor,
            final int numberOfThreads) {
        return addStage(processor, numberOfThreads, bufferSize);
    }

    private <O> PipelineBuilder<I, O> addStage(
            final Processor<T, O> processor,
            final int numberOfThreads,
            final int newBufferSize) {
        final Pipe<O> outputPipe = new Pipe<>(newBufferSize);
        final Stage processStage = new ProcessingStage<>(lastPipe, outputPipe, processor);
        final List<Stage> newStages = concat(stages, processStage);
        return new PipelineBuilder<>(inputPipe, newStages, concat(pipes, outputPipe),
                outputPipe, newBufferSize, numberOfThreads);
    }

    private <O> PipelineBuilder<I, O> addParallelStage(
            final Supplier<Processor<T, O>> supplier) {
        return this.addParallelStage(supplier, numberOfThreads);
    }

    private <O> PipelineBuilder<I, O> addParallelStage(
            final Supplier<Processor<T, O>> supplier,
            final int numberOfThreads) {
        return this.addParallelStage(supplier, numberOfThreads, bufferSize);
    }

    private <O> PipelineBuilder<I, O> addParallelStage(
            final Supplier<Processor<T, O>> supplier,
            final int numberOfThreads,
            final int newBufferSize) {
        final Pipe<O> newLastPipe = new Pipe<>(newBufferSize);
        final WritePipe<O> outputPipe = new SharedWritePipe<>(newLastPipe, numberOfThreads);
        final ArrayList<Stage> newStages = new ArrayList<>(stages);
        for (int i = 0; i < numberOfThreads; i++) {
            final Stage processStage = new ProcessingStage<>(lastPipe, outputPipe, supplier.get());
            newStages.add(processStage);
        }
        return new PipelineBuilder<>(inputPipe, newStages, concat(pipes, newLastPipe),
                newLastPipe, newBufferSize, numberOfThreads);
    }

    private <X> List<X> concat(final Collection<X> existing, final X newItem) {
        final List<X> newList = new ArrayList<>(existing);
        newList.add(newItem);
        return newList;
    }
}
