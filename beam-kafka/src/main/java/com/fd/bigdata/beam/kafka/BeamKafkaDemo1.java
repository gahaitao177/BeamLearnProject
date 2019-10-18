package com.fd.bigdata.beam.kafka;

import avro.shaded.com.google.common.collect.ImmutableMap;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;

/**
 * @author Socean
 * @date 2019/10/17 9:48
 */
public class BeamKafkaDemo1 {
    public static void main(String[] args) {
        //创建管道工厂
        PipelineOptions options = PipelineOptionsFactory.create();

        // 显式指定PipelineRunner：FlinkRunner（Local模式）
        options.setRunner(DirectRunner.class);
        //options.setRunner(FlinkRunner.class);

        //设置相关管道
        Pipeline pipeline = Pipeline.create(options);

        //读取kafka数据集
        PCollection<KafkaRecord<String, String>> lines = pipeline.apply(KafkaIO.<String, String>read().withBootstrapServers("101.201.56.77:9092")
                .withTopic("TopicAlarm")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .updateConsumerProperties(ImmutableMap.<String, Object>of("auto.offset.reset", "earliest")));

        //设置Scheam的字段名称和类型
        final Schema type = Schema.of(Schema.Field.of("alarmid", Schema.FieldType.STRING), Schema.Field.of("alarmTitle", Schema.FieldType.STRING),
                Schema.Field.of("deviceModel", Schema.FieldType.STRING), Schema.Field.of("alarmSource", Schema.FieldType.INT32), Schema.Field.of("alarmMsg", Schema.FieldType.STRING));

        //从kafka中读出的数据
        PCollection<KV<String, String>> kafkadata = lines.apply("Remove Kafka Metadata", ParDo.of(new DoFn<KafkaRecord<String, String>, KV<String, String>>() {
            private static final long serialVersionUID = 1L;

            @ProcessElement
            public void processElement(ProcessContext ctx) {
                //这里将kafka中的消息进行返回操作
                ctx.output(ctx.element().getKV());
            }
        }));

        PCollection<String> resultdata = kafkadata.apply("concat results data", MapElements.via(new SimpleFunction<KV<String, String>, String>() {
            @Override
            public String apply(KV<String, String> input) {
                return input.getKey() + " # " + input.getValue();
            }
        }));

        resultdata.apply(KafkaIO.<Void, String>write()
                //设置写回kafka集群地址和端口
                .withBootstrapServers("10.192.32.202:11092,10.192.32.202:12092,10.192.32.202:13092")
                //设置写回kafka的topic
                .withTopic("send-kafka-msg")
                //这里不用设置了，因为上面key是Void
                //.withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class)
                //.withEOS(20, "eos-sink-group-id")
                .values());

        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception e) {
            try {
                result.cancel();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

    }
}
