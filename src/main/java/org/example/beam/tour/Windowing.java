package org.example.beam.tour;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Sometimes, we want to aggregate data, like GroupByKey or Combine, only at certain intervals, like hourly or daily, instead of processing the entire PCollection of data only once.
 * We might want to emit a moving average as we're processing data.
 * Maybe we want to analyze the user experience for a certain task in a web app, it would be nice to get the app events by sessions of activity.
 * Or we could be running a streaming pipeline, and there is no end to the data, so how can we aggregate data?
 * Windows in Beam allow us to process only certain data intervals at a time. In this notebook, we go through different ways of windowing our pipeline.
 */
@Slf4j
public class Windowing {
    static void process(List<KV<LocalDateTime, String>> input, Window<KV<LocalDateTime, String>> window) {
        Pipeline p = Pipeline.create();

        p.apply("input", Create.of(input))
            .apply("Convert to TimestampedValue",
                WithTimestamps.of(kv -> new Instant(kv.getKey().toEpochSecond(ZoneOffset.UTC) * 1_000L)))
            .apply(window)
            .apply("print window info", ParDo.of(new DoFn<KV<LocalDateTime, String>, KV<LocalDateTime, String>>() {
                @ProcessElement
                public void processElement(ProcessContext c, BoundedWindow window) {
                    // Windowの情報を取得する
                    String windowType = window.getClass().getSimpleName();
                    log.info("Window Type: {}, Window: {}", windowType, window);
                    c.output(c.element());
                }
            }))
            .apply("kv to string", MapElements.into(TypeDescriptors.strings())
                .via(input1 -> input1.getKey() + ": " + input1.getValue()))
            .apply(new PTransform<PCollection<String>, PCollection<Long>>() {
                @Override
                public PCollection<Long> expand(PCollection<String> input) {
                    return input
                        .apply("Count elements per window", Combine.globally(Count.<String>combineFn()).withoutDefaults())
                        ;
                }
            })
            .apply("print result", ParDo.of(new DoFn<Long, Void>() {
                @ProcessElement
                public void processElement(ProcessContext c, BoundedWindow window) {
                    log.info("Window: {}, count: {}", window, c.element());
                }
            }))
        ;

        p.run().waitUntilFinish();
    }

    public static void main(String... args) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        List<KV<LocalDateTime, String>> input = List.of(
            KV.of(LocalDateTime.parse("2021-03-20 03:37:00", formatter), "March Equinox 2021"),
            KV.of(LocalDateTime.parse("2021-04-26 22:31:00", formatter), "Super full moon"),
            KV.of(LocalDateTime.parse("2021-05-11 13:59:00", formatter), "Micro new moon"),
            KV.of(LocalDateTime.parse("2021-05-26 06:13:00", formatter), "Super full moon, total lunar eclipse"),
            KV.of(LocalDateTime.parse("2021-06-20 22:32:00", formatter), "June Solstice 2021"),
            KV.of(LocalDateTime.parse("2021-08-22 07:01:00", formatter), "Blue moon"),
            KV.of(LocalDateTime.parse("2021-09-22 14:21:00", formatter), "September Equinox 2021"),
            KV.of(LocalDateTime.parse("2021-11-04 15:14:00", formatter), "Super new moon"),
            KV.of(LocalDateTime.parse("2021-11-19 02:57:00", formatter), "Micro full moon, partial lunar eclipse"),
            KV.of(LocalDateTime.parse("2021-12-04 01:43:00", formatter), "Super new moon"),
            KV.of(LocalDateTime.parse("2021-12-18 10:35:00", formatter), "Micro full moon"),
            KV.of(LocalDateTime.parse("2021-12-21 09:59:00", formatter), "December Solstice 2021")
        );

        Window<KV<LocalDateTime, String>> fixed =
            Window.<KV<LocalDateTime, String>>into(FixedWindows.of(Duration.standardDays(3 * 30)))
                .withAllowedLateness(Duration.standardSeconds(10))
                .accumulatingFiredPanes()
                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardMinutes(1))));

        Window<KV<LocalDateTime, String>> sliding =
            Window.<KV<LocalDateTime, String>>into(SlidingWindows.of(Duration.standardDays(3 * 30)).every(Duration.standardDays(30)))
                .withAllowedLateness(Duration.standardSeconds(10))
                .accumulatingFiredPanes()
                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardMinutes(1))));

        Window<KV<LocalDateTime, String>> session =
            Window.<KV<LocalDateTime, String>>into(Sessions.withGapDuration(Duration.standardDays(30)))
                .withAllowedLateness(Duration.standardSeconds(10))
                .accumulatingFiredPanes()
                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardMinutes(1))));

        process(input, fixed);
        process(input, sliding);
        process(input, session);
    }
}
