package org.example.beam.tour;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.transforms.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class Partition {
    @Data
    @Builder
    @DefaultCoder(AvroCoder.class)
    @NoArgsConstructor
    @AllArgsConstructor
    static class Model {
        String name;
        int age;
        int type;
    }

    public static void main(String... args) {
        Schema schema = ReflectData.get().getSchema(Model.class);

        Pipeline p = Pipeline.create();
        List<Model> input = List.of(
            Model.builder().name("include").age(1).type(1).build(),
            Model.builder().name("test").age(1).type(2).build(),
            Model.builder().name("z3O9L").age(2).type(2).build(),
            Model.builder().name("VFeJZ81N").age(2).type(2).build(),
            Model.builder().name("ozZ932A").age(3).type(2).build(),
            Model.builder().name("desire").age(6).type(2).build(),
            Model.builder().name("polite").age(7).type(2).build(),
            Model.builder().name("door").age(7).type(2).build()
        );
        var pCollections = p
            .apply("input", Create.of(input))
            .apply(ParDo.of(new DoFn<Model, Model>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    c.output(c.element());
                }
            }))
            .apply(org.apache.beam.sdk.transforms.Partition.of(10, (elem, numPartitions) -> {
                return elem.age * numPartitions / 10;
            }));

        AtomicInteger atomicInteger = new AtomicInteger();
        pCollections.getAll()
            .forEach(modelPCollection -> {
                modelPCollection.apply(ParDo.of(new DoFn<Model, Void>() {
                    final int i = atomicInteger.getAndIncrement();
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        log.info("partition: {}, element: {}", i, c.element());
                    }
                }));
            });

        p.run().waitUntilFinish();
    }
}
