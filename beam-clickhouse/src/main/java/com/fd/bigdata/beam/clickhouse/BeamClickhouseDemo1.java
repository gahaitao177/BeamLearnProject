package com.fd.bigdata.beam.clickhouse;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.fd.bigdata.beam.entities.AlarmTable;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.clickhouse.ClickHouseIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.gson.Gson;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

/**
 * @author Socean
 * @date 2019/10/17 13:45
 */
public class BeamClickhouseDemo1 {
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

        PCollection<Row> modelPCollection = kafkadata.setCoder(AvroCoder.of(AlarmTable.class))
                .apply("object transfrom to Row ", ParDo.of(new DoFn<AlarmTable, Row>() {
                    private static final long serialVersionUID = 1l;

                    @ProcessElement
                    public void processElement(ProcessContext cxt) {
                        AlarmTable alarmTable = cxt.element();
                        Row alarmRow = Row.withSchema(type).addValues(alarmTable.getAlarmid(), alarmTable.getAlarmTitle(), alarmTable.getDeviceModel(), alarmTable.getAlarmSource(), alarmTable.getAlarmMsg()).build();
                        cxt.output(alarmRow);
                    }
                }));

        modelPCollection.setRowSchema(type).apply(ClickHouseIO.<Row>write("jdbc:clickhouse://192.168.1.77:8123/Alarm", "AlarmTable")
                .withMaxRetries(3)//重试次数
                .withMaxInsertBlockSize(5)//添加最大块的大小
                .withInitialBackoff(Duration.standardSeconds(5))//初始退回时间
                .withInsertDeduplicate(true)//重复数据是否删除
                .withInsertDistributedSync(false));

        pipeline.run().waitUntilFinish();

    }
}
