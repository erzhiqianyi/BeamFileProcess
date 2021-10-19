package top.erzhiqian.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class Summary {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> input = pipeline.apply(TextIO.read().from("pom.xml"));
        input.apply(
                TextIO.write().to("C:\\Users\\erzhiqianyi\\IdeaProjects\\BeamFileProcess\\src\\output\\")
                        .withSuffix(".txt")
                        .withNumShards(1));
        pipeline.run();
    }
}
