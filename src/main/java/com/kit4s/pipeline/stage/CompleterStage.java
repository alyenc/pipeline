package com.kit4s.pipeline.stage;

import com.kit4s.pipeline.pipe.ReadPipe;
import lombok.Getter;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class CompleterStage<T> implements Stage {

    private final ReadPipe<T> input;

    private final Consumer<T> completer;
    @Getter
    private final CompletableFuture<?> future = new CompletableFuture<>();

    public CompleterStage(ReadPipe<T> input, Consumer<T> completer) {
        this.input = input;
        this.completer = completer;
    }

    @Override
    public void run() {
        while (input.hasMore()) {
            final T value = input.get();
            if (value != null) {
                completer.accept(value);
            }
        }
        future.complete(null);
    }
}
