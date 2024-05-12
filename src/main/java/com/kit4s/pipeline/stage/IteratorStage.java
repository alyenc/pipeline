package com.kit4s.pipeline.stage;

import com.kit4s.pipeline.pipe.Pipe;

import java.util.Iterator;

public class IteratorStage<T> implements Stage {
    private final Iterator<T> source;

    private final Pipe<T> pipe;

    public IteratorStage(final Iterator<T> source, final Pipe<T> pipe) {
        this.source = source;
        this.pipe = pipe;
    }

    @Override
    public void run() {
        while (pipe.isOpen() && source.hasNext()) {
            final T value = source.next();
            if (value != null) {
                pipe.put(value);
            }
        }
        pipe.close();
    }
}
