package com.fd.bigdata.beam.practice;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

/**
 * @author Socean
 * @date 2019/10/17 19:03
 */
public class BeamPracticeDemo1 {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from("/home/jbonofre/artists.csv"))
                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext processContext) {
                        String element = processContext.element();
                        String[] split = element.split(",");
                        processContext.output(split[1]);
                    }
                }))
                .apply(Count.<String>perElement())
                .apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> element) {
                        return "{\"" + element.getKey() + "\": \"" + element.getValue() + "\"}";
                    }
                })).apply(TextIO.write().to("/home/jbonofre/label.json"));

        pipeline.run().waitUntilFinish();
    }
}
