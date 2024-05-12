package com.kit4s.pipeline.processor;

import com.kit4s.pipeline.pipe.ReadPipe;
import com.kit4s.pipeline.pipe.WritePipe;

public interface Processor<I, O> {

    void process(final ReadPipe<I> inputPipe, final WritePipe<O> outputPipe);

    default boolean attemptFinalization(final WritePipe<O> outputPipe) {
        return true;
    }

    default void abort() {}
}
