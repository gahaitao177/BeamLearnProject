package com.fd.bigdata.beam.s3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Socean
 * @date 2019/10/17 17:26
 */
public class BeamS3Demo1 {
    private static final Logger LOG = LoggerFactory.getLogger(BeamS3Demo1.class);

    public static void main(String[] args) {
        //创建管道工厂
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        if (options.getInput() == null) {
            options.setInput(Options.GDELT_EVENTS_URL + options.getDate() + ".export.csv");
        }

        LOG.info(options.toString());

        //设置相关管道
        Pipeline pipeline = Pipeline.create(options);

        //读取数据
        PCollection<String> readFromGDELFile = pipeline.apply("ReadFromGDELFile", TextIO.read().from(options.getInput()));

        //写入到S3
        PDone writeToFS = readFromGDELFile.apply("WriteToFS", TextIO.write().to(options.getOutPut()));

        pipeline.run().waitUntilFinish();
    }

    /**
     * 指定pipeline 的options
     */
    public interface Options extends S3Options {
        String GDELT_EVENTS_URL = "s3://gdelt-open-data/events/";

        @Description("GDELT file date")
        @Default.InstanceFactory(Options.GDELTFileFactory.class)
        String getDate();

        void setDate(String value);

        @Description("Input Path")
        String getInput();

        void setInput(String value);

        @Description("Output Path")
        String getOutPut();

        void setOutput(String value);

        class GDELTFileFactory implements DefaultValueFactory<String> {

            public String create(PipelineOptions options) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                return format.format(new Date());
            }
        }
    }
}
