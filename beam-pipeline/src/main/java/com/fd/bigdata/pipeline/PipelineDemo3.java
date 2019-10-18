package com.fd.bigdata.pipeline;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.Arrays;
import java.util.List;

/**
 * @author Socean
 * @date 2019/10/17 9:23
 */
public class PipelineDemo3 {
    public static void main(String[] args) {
        //创建管道工厂
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);

        //设置相关管道
        Pipeline pipeline = Pipeline.create(options);

        final List<String> LINESa = Arrays.asList("Aggressive", "Apprehensive");
        final List<String> LINESb = Arrays.asList("Bold", "Brilliant");

        PCollection<String> aCollection = pipeline.apply(Create.of(LINESa)).setCoder(StringUtf8Coder.of());
        PCollection<String> bCollection = pipeline.apply(Create.of(LINESb)).setCoder(StringUtf8Coder.of());

        //将两个PCollection与Flatten合并
        PCollectionList<String> collectionList = PCollectionList.of(aCollection).and(bCollection);

        PCollection<String> mergedCollectionWithFlatten = collectionList.apply(Flatten.<String>pCollections());

        System.out.append("合并的单词有：\r");
        //设置管道的数据集
        PCollection<String> resultCollection = mergedCollectionWithFlatten.apply("aTrans", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext cxt) {
                cxt.output(cxt.element());
                System.out.append(cxt.element() + "\r");
            }
        }));

        resultCollection.apply(TextIO.write().to("D:\\wordcount\\B-pipeline"));

        pipeline.run().waitUntilFinish();
    }
}
