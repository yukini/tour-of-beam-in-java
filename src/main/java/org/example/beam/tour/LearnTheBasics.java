package org.example.beam.tour;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.example.beam.tour.function.PrintFn;

import java.util.ArrayList;
import java.util.List;

/**
 * In this notebook we go through the basics of what is Apache Beam and how to get started.
 * We learn what is a data pipeline, a PCollection, a PTransform,
 * as well as some basic transforms like Map, FlatMap, Filter, Combine, and GroupByKey.
 */
@Slf4j
public class LearnTheBasics {

    static void map() {
        Pipeline p = Pipeline.create();
        List<Integer> input = List.of(0, 1, 2, 3);
        p
            .apply("first step", Create.of(input))
//            .apply("map", MapElements.into(TypeDescriptors.integers()).via(i -> i + 1))
//            .apply("map 2", MapElements.into(TypeDescriptors.integers()).via(i -> i * 2))
            .apply(new MapTransform())
            .apply("print", ParDo.of(new PrintFn()))
        ;
        p.run().waitUntilFinish();
    }

    static class MapTransform extends PTransform<@NonNull PCollection<Integer>, @NonNull PCollection<Integer>> {
        @Override
        @NonNull
        public PCollection<Integer> expand(PCollection<Integer> input) {
            return input
                .apply("map", MapElements.into(TypeDescriptors.integers()).via(i -> i + 1))
                .apply("map 2", MapElements.into(TypeDescriptors.integers()).via(i -> i * 2));
        }
    }

    static void flatMap() {
        Pipeline p = Pipeline.create();
        List<Integer> input = List.of(0, 1, 2, 3);
        p
            .apply("first step", Create.of(input))
            .apply(FlatMapElements.into(TypeDescriptors.integers())
                .via(x -> {
                    List<Integer> output = new ArrayList<>();
                    for (int i = 0; i < x; i++) {
                        output.add(x);
                    }
                    return output;
                }))
            .apply("print", ParDo.of(new PrintFn()))
        ;
        p.run().waitUntilFinish();
    }

    static void filter() {
        Pipeline p = Pipeline.create();
        List<Integer> input = List.of(0, 1, 2, 3);
        p
            .apply("first step", Create.of(input))
            .apply(Filter.by(i -> i % 2 == 0))
            .apply("print", ParDo.of(new PrintFn()))
        ;
        p.run().waitUntilFinish();
    }

    static void combine() {
        Pipeline p = Pipeline.create();
        List<Integer> input = List.of(0, 1, 2, 3);
        p
            .apply("first step", Create.of(input))
            .apply(Filter.by(i -> i % 2 == 0))
            .apply(Combine.globally(Integer::sum))
            .apply("print", ParDo.of(new PrintFn()))
        ;
        p.run().waitUntilFinish();
    }

    static void groupByKey() {
        Pipeline p = Pipeline.create();
        p.apply("input", Create.of(
                KV.of("🐹", "🌽"),
                KV.of("🐼", "🎋"),
                KV.of("🐰", "🥕"),
                KV.of("🐹", "🌰"),
                KV.of("🐰", "🥒")
            ))
            .apply(GroupByKey.create())
            .apply(ParDo.of(new PrintFn()))
        ;
        p.run().waitUntilFinish();
    }

    public static void main(String... args) {
        map();
//        flatMap();
//        filter();
//        combine();
//        groupByKey();
    }
}
