package org.example.beam.tour;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.example.beam.tour.function.PrintFn;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * In this notebook we go through some examples on how to read and write data to and from different data formats.
 * We introduce the built-in ReadFromText and WriteToText transforms.
 * We also see how we can read from CSV files, read from a SQLite database, write fixed-sized batches of elements,
 * and write windows of elements.
 */
@Slf4j
public class ReadingAndWritingData {

    static void readingFromTextFiles() {
        Pipeline p = Pipeline.create();
        p
            .apply("Read files", TextIO.read().from("src/main/resources/data/*.txt"))
            .apply("print", ParDo.of(new PrintFn()))
        ;

        p.run().waitUntilFinish();
    }

    static void writingToTextFiles() {
        List<String> lines = Arrays.asList(
            "Each element must be a string.",
            "It writes one element per line.",
            "There are no guarantees on the line order.",
            "The data might be written into multiple files.");

        Pipeline p = Pipeline.create();
        p
            .apply("Create file lines", Create.of(lines))
            .apply("Write to files", TextIO.write().to("outputs/file").withSuffix(".txt"))
        ;

        p.run().waitUntilFinish();
    }

    static void readingFromAnIterable() {
        List<String> lines = Arrays.asList(
            "Each element must be a string.",
            "It writes one element per line.",
            "There are no guarantees on the line order.",
            "The data might be written into multiple files.");

        SerializableFunction<Integer, Iterable<Integer>> countGenerator = (n) -> () -> new Iterator<>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < n;
            }

            @Override
            public Integer next() {
                return i++;
            }
        };

        Pipeline p = Pipeline.create();
        p
            .apply("input", Create.of(5))
            .apply(
                FlatMapElements.into(TypeDescriptors.integers())
                    .via(countGenerator))
            .apply("print", ParDo.of(new PrintFn()))
        ;

        p.run().waitUntilFinish();
    }

    static class Count implements SerializableFunction<Integer, Iterable<Integer>> {
        @Override
        public Iterable<Integer> apply(Integer n) {
            return () -> {
                return new Iterator<Integer>() {
                    int i = 0;

                    @Override
                    public boolean hasNext() {
                        return i < n;
                    }

                    @Override
                    public Integer next() {
                        return i++;
                    }
                };
            };
        }
    }

    static void creatingAnInputTransform() {
        int n = 5;

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply(Create.of(n))
            .apply(FlatMapElements.into(TypeDescriptors.integers())
                .via(new Count()))
            .apply(ParDo.of(new PrintFn()))
        ;

        pipeline.run().waitUntilFinish();
    }

    static void readingCSVFiles() {
        throw new UnsupportedOperationException("CSV読み込んでMapに詰めるだけ、現実的にはModelに詰める。");
    }

    public static void main(String... args) {
        readingFromAnIterable();
    }
}
