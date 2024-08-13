package com.kit4s.pipeline.task;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public interface Task<T> {

    CompletableFuture<T> run();

    CompletableFuture<T> runAsync(ExecutorService executor);

    void cancel();
}
