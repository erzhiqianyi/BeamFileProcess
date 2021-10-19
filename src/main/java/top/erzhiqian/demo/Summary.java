package top.erzhiqian.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.beam.sdk.values.TypeDescriptors.lists;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class Summary {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> output =
                pipeline
                        .apply(FileIO.match().filepattern("C:\\Users\\erzhiqianyi\\IdeaProjects\\BeamFileProcess\\src\\input\\Mars.log*"))
                        .apply(FileIO.readMatches())
                        .apply(FlatMapElements
                                .into(strings())
                                .via((FileIO.ReadableFile f) -> {
                                    List<String> result = new ArrayList<>();
                                    try (BufferedReader br = new BufferedReader(Channels.newReader(f.open(), "UTF-8"))) {
                                        String line = br.readLine();
                                        while (line != null) {
                                            result.add(line);
                                            line = br.readLine();
                                        }
                                    } catch (IOException e) {
                                        throw new RuntimeException("Error while reading", e);
                                    }
                                    List<String> targetResult = new ArrayList<>();
                                    StringBuffer file = new StringBuffer();
                                    for (String s : result) {
                                        if (s.startsWith("start_time:")) {
                                            file.append(s).append(",");
                                        } else if (s.startsWith(" Full")) {
                                            file.append(s).append(",");
                                        } else if (s.startsWith("end_time")) {
                                            file.append(s);
                                            targetResult.add(file.toString());
                                            file = new StringBuffer();
                                        }
                                    }
                                    return targetResult;
                                }));

        output.apply("FormatResults", MapElements.into(TypeDescriptors.strings())
                        .via((String line) ->
                                line.replace("start_time:", "")
                                        .replace("end_time:", "")
                                        .replace("FullFile:", "")
                                        .replace(" ", "")
                        )).
                apply(TextIO.write().to("C:\\Users\\erzhiqianyi\\IdeaProjects\\BeamFileProcess\\src\\output\\")
                        .withSuffix(".txt")
                        .withNumShards(1));
        pipeline.run();
    }
}
