package com.kit4s.pipeline.processor;

import com.kit4s.pipeline.pipe.ReadPipe;
import com.kit4s.pipeline.pipe.WritePipe;

import java.util.function.Function;
import java.util.stream.Stream;

public class FlatMapProcessor<I, O> implements Processor<I, O> {

    private final Function<I, Stream<O>> mapper;

    public FlatMapProcessor(final Function<I, Stream<O>> mapper) {
        this.mapper = mapper;
    }

    @Override
    public void process(final ReadPipe<I> inputPipe, final WritePipe<O> outputPipe) {
        final I value = inputPipe.get();
        if (value != null) {
            mapper.apply(value).forEach(outputPipe::put);
        }
    }
}
