package com.kit4s.pipeline.processor;

import com.kit4s.pipeline.pipe.ReadPipe;
import com.kit4s.pipeline.pipe.WritePipe;

import java.util.function.Function;

public class MapProcessor<I, O> implements Processor<I, O>{

    private final Function<I, O> function;

    public MapProcessor(final Function<I, O> function) {
        this.function = function;
    }

    @Override
    public void process(ReadPipe<I> inputPipe, WritePipe<O> outputPipe) {
        final I value = inputPipe.get();
        if (value != null) {
            outputPipe.put(function.apply(value));
        }
    }
}
