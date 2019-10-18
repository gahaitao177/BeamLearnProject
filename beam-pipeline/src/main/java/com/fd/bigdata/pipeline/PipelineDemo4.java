package com.fd.bigdata.pipeline;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Arrays;
import java.util.List;

/**
 * @author Socean
 * @date 2019/10/17 10:57
 */
public class PipelineDemo4 {
    public static void main(String[] args) {
        //创建管道工厂
        PipelineOptions options = PipelineOptionsFactory.create();
        //显示指定PipelineRunner
        options.setRunner(DirectRunner.class);

        //设置相关管道
        Pipeline pipeline = Pipeline.create(options);

        //为了演示,准备数据集
        //叫号数据
        final List<KV<String, String>> txtnoticelist = Arrays.asList(
                KV.of("DS-2CD2326FWDA3-I", "101号顾客请到3号柜台"),
                KV.of("DS-2CD2T26FDWDA3-IS", "102号顾客请到1号柜台"),
                KV.of("DS-2CD6984F-IHS", "103号顾客请到4号柜台"),
                KV.of("DS-2CD7627HWD-LZS", "104号顾客请到2号柜台"));

        //AI行为分析消息
        final List<KV<String, String>> aimessagelist = Arrays.asList(
                KV.of("DS-2CD2326FWDA3-I", "CMOS智能半球网络摄像机,山东省济南市解放路支行3号柜台,type=2,display_image=no"),
                KV.of("DS-2CD2T26FDWDA3-IS", "CMOS智能筒型网络摄像机,山东省济南市甸柳庄支行1号柜台,type=2,display_image=no"),
                KV.of("DS-2CD6984F-IHS", "星光级全景拼接网络摄像机,山东省济南市市中区支行4号柜台,type=2,display_image=no"),
                KV.of("DS-2CD7627HWD-LZS", "全结构化摄像机,山东省济南市市中区支行2号柜台,type=2,display_image=no"));

        PCollection<KV<String, String>> notice = pipeline.apply("CreateEmails", Create.of(txtnoticelist));
        PCollection<KV<String, String>> message = pipeline.apply("CreatePhones", Create.of(aimessagelist));
        final TupleTag<String> noticeTag = new TupleTag<String>();
        final TupleTag<String> messageTag = new TupleTag<String>();

        PCollection<KV<String, CoGbkResult>> results = KeyedPCollectionTuple.of(noticeTag, notice).and(messageTag, message).apply(CoGroupByKey.<String>create());
        System.out.println("合并分组后的结果：\r");

        PCollection<String> result = results.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
            private static final long serialVersionUID = 1L;

            @ProcessElement
            public void processElement(ProcessContext cxt) {
                KV<String, CoGbkResult> e = cxt.element();
                String name = e.getKey();
                Iterable<String> emailsIter = e.getValue().getAll(noticeTag);
                Iterable<String> phonesIter = e.getValue().getAll(messageTag);
                System.out.append("" + name + ";" + emailsIter + ";" + phonesIter + ";" + "\r");

                cxt.output("" + name + ";" + emailsIter + ";" + phonesIter + ";" + "\r");
            }
        }));

        result.apply(TextIO.write().to("D:\\wordcount\\demo4"));

        pipeline.run().waitUntilFinish();
    }
}
