package com.kit4s.pipeline.task;

public interface TasksPriorityProvider {

    long getPriority();

    int getDepth();
}
