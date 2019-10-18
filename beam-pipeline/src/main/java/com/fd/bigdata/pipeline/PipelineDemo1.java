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

import java.util.Arrays;
import java.util.List;

/**
 * @author Socean
 * @date 2019/10/16 19:39
 */
public class PipelineDemo1 {
    public static void main(String[] args) {
        // 创建管道工厂
        PipelineOptions options = PipelineOptionsFactory.create();
        // 显式指定PipelineRunner：FlinkRunner必须指定如果不制定则为本地
        options.setRunner(DirectRunner.class); // 显式指定PipelineRunner：FlinkRunner（Local模式）//必须指定如果不制定则为本地
        // options.setRunner(FlinkRunner.class); //
        // 显式指定PipelineRunner：FlinkRunner（Local模式）//必须指定如果不制定则为本地
        Pipeline pipeline = Pipeline.create(options);// 设置相关管道
        //为了演示显示内存数据集
        final List<String> LINES = Arrays.asList(
                "Aggressive",//有进取心的
                "Bold",//大胆的,勇敢的
                "Apprehensive",//有理解能力的
                "Brilliant");//才华横溢的

        //设置管道的数据集
        PCollection<String> dbRowCollection = pipeline.apply(
                Create.of(LINES)).setCoder(StringUtf8Coder.of());

        PCollection<String> aCollection = dbRowCollection.apply("aTrans", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {

                if (c.element().startsWith("A")) {//查找以"A"开头的数据
                    c.output(c.element());
                    System.out.println("A---------A");
                    System.out.append("A开头的单词有:" + c.element() + "\r");
                }
            }
        }));

        aCollection.apply(TextIO.write().to("D:\\wordcount\\Atxt"));

        PCollection<String> bCollection = dbRowCollection.apply("bTrans", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {

                if (c.element().startsWith("B")) {//查找以"B"开头的数据
                    c.output(c.element());
                    System.out.println("B---------B");
                    System.out.append("B开头的单词有:" + c.element() + "\r");
                }
            }
        }));

        bCollection.apply(TextIO.write().to("D:\\wordcount\\Btxt"));

        pipeline.run().waitUntilFinish();
    }
}
