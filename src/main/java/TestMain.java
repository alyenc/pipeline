import com.kit4s.pipeline.Pipeline;
import com.kit4s.pipeline.PipelineBuilder;
import com.kit4s.pipeline.pipe.Pipe;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

@Slf4j
public class TestMain {

    public static void main(String[] args) {
        Pipeline<String> pipeline = PipelineBuilder.<String>createPipeline(10)
                .thenProcess(str -> {
                    log.info(str);
                    return str + "!";
                })
                .andFinishWith(log::info);

        final Pipe<String> input = pipeline.getInputPipe();
        input.put("Hello World");
        CompletableFuture<Void> pipelineFuture = pipeline.start(Executors.newSingleThreadExecutor());
        pipelineFuture.whenComplete((r, t) -> {
            System.out.println("aaaa");
        });
    }
}
