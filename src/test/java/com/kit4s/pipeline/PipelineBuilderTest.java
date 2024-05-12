package com.kit4s.pipeline;

import com.kit4s.pipeline.pipe.Pipe;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

@Slf4j
public class PipelineBuilderTest {

    @Test // should be not null
    public void createPipeline() {
        StringSource strings = new StringSource();

        Pipeline<String> pipeline = PipelineBuilder.createPipeline(strings,10)
                .thenProcess(str -> str + "!")
                .andFinishWith(System.out::println);
        pipeline.start(Executors.newSingleThreadExecutor());
    }

    static class StringSource implements Iterator<String> {
        private int i = 0;
        private final String[] strings = new String[]{"Hello", "World", "!"};

        @Override
        public boolean hasNext() {
            return i < strings.length;
        }

        @Override
        public String next() {
            return strings[i++];
        }
    }
}
