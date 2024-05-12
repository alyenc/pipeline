package com.kit4s.pipeline.stage;

import com.kit4s.pipeline.pipe.ReadPipe;
import com.kit4s.pipeline.pipe.WritePipe;
import com.kit4s.pipeline.processor.Processor;

public class ProcessingStage<I, O> implements Stage {

    private final ReadPipe<I> inputPipe;

    private final WritePipe<O> outputPipe;

    private final Processor<I, O> processor;

    public ProcessingStage(
            final ReadPipe<I> inputPipe,
            final WritePipe<O> outputPipe,
            final Processor<I, O> processor) {
        this.inputPipe = inputPipe;
        this.outputPipe = outputPipe;
        this.processor = processor;
    }

    @Override
    public void run() {
        while (inputPipe.hasMore()) {
            processor.process(inputPipe, outputPipe);
        }
        if (inputPipe.isAborted()) {
            processor.abort();
        }
        while (!processor.attemptFinalization(outputPipe)) {
            if (inputPipe.isAborted()) {
                processor.abort();
                break;
            }
        }
        outputPipe.close();
    }
}
