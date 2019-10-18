package com.fd.bigdata.beam.hdfs;

import com.google.common.collect.ImmutableList;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Socean
 * @date 2019/10/17 14:47
 */
public class BeamHDFSDemo1 {
    public static void main(String[] args) {
        //配置hadoop的配置文件
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.220.140:9000");

        HadoopFileSystemOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(HadoopFileSystemOptions.class);
        options.setHdfsConfiguration(ImmutableList.of(conf));
        options.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);
        //根据文件路径读取文件内容
        PCollection<String> resultPCollection = pipeline.apply(TextIO.read().from("hdfs://192.168.220.140:9000/user/lenovo/testfile/test.txt"))
                .apply("Read message from hdfs", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext cxt) {
                        for (String word : cxt.element().split(" ")) {
                            if (!word.isEmpty()) {
                                cxt.output(word);
                            }
                        }
                    }
                }));

        pipeline.run().waitUntilFinish();
    }
}
