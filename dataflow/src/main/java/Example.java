//import io.grpc.internal.JsonUtil;
//import org.apache.beam.runners.direct.DirectRunner;
//import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
//import org.apache.beam.runners.direct.DirectOptions;
//import org.apache.beam.sdk.Pipeline;
//import org.apache.beam.sdk.extensions.gcp.util.UploadIdResponseInterceptor;
//import org.apache.beam.sdk.io.TextIO;
//import org.apache.beam.sdk.options.Default;
//import org.apache.beam.sdk.options.Description;
//import org.apache.beam.sdk.options.PipelineOptions;
//import org.apache.beam.sdk.options.PipelineOptionsFactory;
//import org.apache.beam.sdk.transforms.Create;
//
//import java.util.Arrays;
//import java.util.List;
//
//public class Example {
//    public static interface MyOptions extends DataflowPipelineOptions {
//
//
//    }
//    public static void main(String[] args) {
//
//        final String RESOURCES = "/home/nghiaht7/data-engineer/apache-beam-java/beam-step-by-step/src/main/resources";
//
//        // direct runner on local
////        PipelineOptions pipelineOptions= PipelineOptionsFactory.create();
////        pipelineOptions.setRunner(DirectRunner.class);
//
//        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
////        options.setStreaming(true);
//        options.setJobName("lab2");
//        options.setProject("dtc-de-zoomcamp-339213");
//        options.setRegion("europe-west6");
//        options.setRunner(DirectRunner.class);
//        options.setGcpTempLocation("gs://dtc_data_lake_dtc-de-zoomcamp-339213/tmp");
//
//        Pipeline pipeline = Pipeline.create(options);
//
//
//
//
////        Pipeline pipeline = Pipeline.create(pipelineOptions);
//        final List<String> input = Arrays.asList("First", "Second", "Third", "last");
//
//        pipeline.apply(Create.of(input)).apply(TextIO.write().to(RESOURCES + "output").withSuffix(".txt"));
//
//        pipeline.apply(Create.of(input)).apply(TextIO.write().to("gs://dtc_data_lake_dtc-de-zoomcamp-339213/results").withSuffix(".txt"));
//
//
//
////
////        pipeline.run().waitUntilFinish();
//
//    }
//}
