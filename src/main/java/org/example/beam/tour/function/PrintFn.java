package org.example.beam.tour.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;

@Slf4j
public class PrintFn extends DoFn<Object, Void> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        log.info("element: {}", c.element());
    }
}
