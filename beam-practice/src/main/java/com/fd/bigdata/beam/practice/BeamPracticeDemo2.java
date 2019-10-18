package com.fd.bigdata.beam.practice;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Socean
 * @date 2019/10/17 20:07
 */
public class BeamPracticeDemo2 {
    private static final Logger LOG = LoggerFactory.getLogger(BeamPracticeDemo2.class);

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        if (options.getInput() == null) {
            options.setInput(Options.GDELT_EVENTS_URL + options.getDate() + ".export.CSV.zip");
        }
        if (options.getOutput() == null) {
            options.setOutput("/tmp/gdelt-" + options.getDate());
        }
        LOG.info(options.toString());

        Pipeline pipeline = Pipeline.create(options);

        //读取数据
        PCollection<String> readData = pipeline.apply("ReadFromGDELTFile", TextIO.read().from(options.getInput()))
                .apply("TaskASample", Sample.<String>any(10));

        readData.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext cxt) {
                LOG.info(String.format("READ C.ELEMENT |%s|", cxt.element()));
            }
        }));

        //获取地理位置信息
        PCollection<String> extractLocation = readData.apply("ExtractLocation", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext cxt) {
                cxt.output(getCountry(cxt.element()));
            }
        }));

        //过滤地理位置信息
        PCollection<String> filterLocation = extractLocation.apply("FilterValidLocations", Filter.by(new SerializableFunction<String, Boolean>() {
            public Boolean apply(String input) {
                boolean flag = (!input.equals("NA") && input.startsWith("-") && input.length() == 2);
                return flag;
            }
        }));

        //对地理位置进行统计
        PCollection<KV<String, Long>> countByLocation = filterLocation.apply("CountByLocation", Count.<String>perElement());

        //将处理后的信息转换成json
        PCollection<String> convertToJson = countByLocation.apply("ConvertToJson", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {

            @Override
            public String apply(KV<String, Long> input) {
                return "{\"" + input.getKey() + "\":" + input.getValue() + "}";
            }
        }));

        //将结果写出
        convertToJson.apply("WriteResults", TextIO.write().to(options.getOutput()));

        pipeline.run().waitUntilFinish();
    }

    public interface Options extends PipelineOptions {
        String GDELT_EVENTS_URL = "http://data.gdeltproject.org/events/";

        @Description("GDELT file date")
        @Default.InstanceFactory(GDELTFileFactory.class)
        String getDate();

        void setDate(String value);

        @Description("Input Path")
        String getInput();

        void setInput(String value);

        @Description("Output Path")
        String getOutput();

        void setOutput(String value);

        class GDELTFileFactory implements DefaultValueFactory<String> {
            public String create(PipelineOptions options) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                return format.format(new Date());
            }
        }
    }

    private static String getCountry(String row) {
        String[] fields = row.split("\\t+");

        if (fields.length > 22) {
            if (fields[21].length() > 2) {
                return fields[21].substring(0, 1);
            }
            return fields[21];
        }
        return "NA";
    }
}
