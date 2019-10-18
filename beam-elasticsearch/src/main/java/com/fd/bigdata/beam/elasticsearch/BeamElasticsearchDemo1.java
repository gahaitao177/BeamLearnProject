package com.fd.bigdata.beam.elasticsearch;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.fd.bigdata.beam.entities.AlarmTable;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.gson.Gson;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author Socean
 * @date 2019/10/17 14:30
 */
public class BeamElasticsearchDemo1 {
    public static void main(String[] args) {
        //创建管道工厂
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);

        //配置读取kafka的消息
        PCollection<KafkaRecord<String, String>> lines = pipeline.apply("Read kafka message", KafkaIO.<String, String>read()
                .withBootstrapServers("192.168.1.77:9092")
                .withTopic("TopicAlarm")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .updateConsumerProperties(ImmutableMap.<String, Object>of("auto.offset.reset", "earliest")));

        // 设置Schema 的的字段名称和类型
        final Schema type = Schema.of(Schema.Field.of("alarmid", Schema.FieldType.STRING), Schema.Field.of("alarmTitle", Schema.FieldType.STRING),
                Schema.Field.of("deviceModel", Schema.FieldType.STRING), Schema.Field.of("alarmSource", Schema.FieldType.INT32), Schema.Field.of("alarmMsg", Schema.FieldType.STRING));

        PCollection<AlarmTable> kafkadata = lines.apply("Remove Kafka Metadata", ParDo.of(new DoFn<KafkaRecord<String, String>, AlarmTable>() {
            private static final long serialVersionUID = 1L;

            @ProcessElement
            public void processElement(ProcessContext ctx) {
                System.out.println("Kafka序列化之前的数据 ：" + ctx.element().getKV().getValue() + "\r");

                Gson gson = new Gson();
                AlarmTable modelTable = null;
                try {
                    modelTable = gson.fromJson(ctx.element().getKV().getValue(), AlarmTable.class);
                } catch (Exception e) {
                    System.out.println("json序列化出现问题：" + e);
                }
                ctx.output(modelTable);
            }
        }));

        PCollection<String> jsonCollection = kafkadata.setCoder(AvroCoder.of(AlarmTable.class))
                .apply("convert json", ParDo.of(new DoFn<AlarmTable, String>() {
                    private static final long serialVersionUID = 1L;

                    @ProcessElement
                    public void processElement(ProcessContext ctx) {
                        Gson gson = new Gson();
                        String jString = "";
                        try {
                            jString = gson.toJson(ctx.element());
                        } catch (Exception e) {
                            System.out.println("json序列化出现问题 ：" + e);
                        }

                        ctx.output(jString);
                    }
                }));
        //ES 地址信息
        String[] addresses = {"http://192.168.1.77:9200/"};
        jsonCollection.apply("write data to es", ElasticsearchIO.write().withConnectionConfiguration(ElasticsearchIO.ConnectionConfiguration.create(addresses, "alarm", "TopicAlarm")));

        pipeline.run().waitUntilFinish();
    }
}
