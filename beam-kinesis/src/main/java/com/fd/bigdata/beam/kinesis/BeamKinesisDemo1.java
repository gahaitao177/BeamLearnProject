package com.fd.bigdata.beam.kinesis;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.io.kinesis.KinesisRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Socean
 * @date 2019/10/17 19:15
 */
public class BeamKinesisDemo1 {
    private static final int NUM_RECORDS = 10;

    public static void main(String[] args) {
        Instant now = Instant.now();

        final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        List<byte[]> data = prepareData();

        //向Kinesis写数据
        PDone result = pipeline.apply(Create.of(data))
                .apply(KinesisIO.write()
                        .withStreamName(options.getStream())
                        .withPartitionKey(options.getPartitionKey())
                        .withAWSClientsProvider(options.getAccessKey(),
                                options.getSecretKey(),
                                Regions.fromName(options.getRegionName())));


        pipeline.run().waitUntilFinish();

        //从Kinesis读取数据
        pipeline.apply(KinesisIO.read().withStreamName(options.getStream())
                .withAWSClientsProvider(options.getAccessKey(),
                        options.getSecretKey(),
                        Regions.fromName(options.getRegionName()))
                .withMaxNumRecords(data.size())
                .withMaxReadTime(Duration.standardMinutes(1))
                .withInitialPositionInStream(InitialPositionInStream.AT_TIMESTAMP)
                .withInitialTimestampInStream(now)
        ).apply(ParDo.of(new DoFn<KinesisRecord, byte[]>() {
            @ProcessElement
            public void processElement(ProcessContext cxt) {
                KinesisRecord record = cxt.element();
                byte[] data = record.getData().array();
                cxt.output(data);
            }
        }));

        pipeline.run().waitUntilFinish();


    }

    private static List<byte[]> prepareData() {
        ArrayList<byte[]> data = new ArrayList<byte[]>();
        for (int i = 0; i < NUM_RECORDS; i++) {
            data.add(String.valueOf(i).getBytes());
        }
        return data;
    }

    public interface Options extends PipelineOptions {
        @Description("AWS Access Key")
        String getAccessKey();

        void setAccessKey(String value);

        @Description("AWS Secret Key")
        String getSecretKey();

        void setSecretKey(String value);

        @Description("AWS Region Name")
        String getRegionName();

        void setRegionName(String value);

        @Description("Kinesis Stream Name")
        String getStream();

        void setStream(String value);

        @Description("Partition Key")
        @Default.String("pkey")
        String getPartitionKey();

        void setPartitionKey(String value);
    }
}
