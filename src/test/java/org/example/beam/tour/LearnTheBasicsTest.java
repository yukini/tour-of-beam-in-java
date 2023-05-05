package org.example.beam.tour;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

public class LearnTheBasicsTest {

    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    void setUp() {
    }

    void tearDown() {
    }

    @Test
    public void map() {
        List<Integer> input = List.of(0, 1, 2, 3);

        PCollection<Integer> output = p.apply(Create.of(input))
            .apply(new LearnTheBasics.MapTransform());

        PAssert.that(output)
            .containsInAnyOrder(2, 4, 6, 8);
        p.run();
    }
}