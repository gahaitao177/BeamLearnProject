package com.fd.bigdata.pipeline;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.util.Arrays;
import java.util.List;

/**
 * 注释
 * @author Socean
 * @date 2019/10/16 19:56
 */
public class PipelineDemo2 {
    public static void main(String[] args) {
        //创建管道工厂
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);

        //设置相关管道
        Pipeline pipeline = Pipeline.create(options);

        final List<String> LINES = Arrays.asList("Aggressive", "Bold", "Apprehensive", "Brilliant");
        PCollection<String> dbRowCollection = pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());

        final TupleTag<String> startWithATag = new TupleTag<String>() {
        };
        final TupleTag<String> startWithBTag = new TupleTag<String>() {
        };

        PCollectionTuple mixedCollection = dbRowCollection.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext cxt) {
                if (cxt.element().startsWith("A")) {
                    //返回首字母带A的数据集
                    cxt.output(cxt.element());
                    System.out.println("A---------A");
                    System.out.append("A开头的单词有:").append(cxt.element()).append("\r");
                } else if (cxt.element().startsWith("B")) {
                    cxt.output(startWithBTag, cxt.element());
                    System.out.println("B---------B");
                    System.out.append("B开头的单词有:").append(cxt.element()).append("\r");
                }
            }
        })
                // Specify main output. In this example, it is the output
                // with tag startsWithATag.
                .withOutputTags(startWithATag, TupleTagList.of(startWithBTag)));
        PCollection<String> BCollection = mixedCollection.get(startWithATag).apply("test", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext cxt) {
                cxt.output(cxt.element());
            }
        }));

        BCollection.apply(TextIO.write().to("D:\\wordcount\\B"));

        pipeline.run().waitUntilFinish();
    }
}
