import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class StreamingBasic {

    static final TupleTag<Account> parsedMessages = new TupleTag<Account>() {
    };
    static final TupleTag<String> unparsedMessages = new TupleTag<String>() {
    };

    /*
     * The logger to output status messages to.
     */
    private static final Logger LOG = LoggerFactory.getLogger(StreamingBasic.class);



    public interface MyOptions extends PipelineOptions, StreamingOptions, PubsubOptions {
        @Description("Input topic name")
        String getInputTopic();
        void setInputTopic(String inputTopic);

    }


    /**
     * A PTransform accepting Json and outputting tagged CommonLog with Beam Schema or raw Json string if parsing fails
     */
    public static class PubsubMessageToAccount extends PTransform<PCollection<String>, PCollectionTuple> {
        @Override
        public PCollectionTuple expand(PCollection<String> input) {
            return input
                    .apply("JsonToAccount", ParDo.of(new DoFn<String, Account>() {
                                @ProcessElement
                                public void processElement(ProcessContext context) {
                                    String json = context.element();
                                    Gson gson = new Gson();
                                    try {
                                        Account account = gson.fromJson(json, Account.class);
                                        context.output(parsedMessages, account);
                                    } catch (JsonSyntaxException e) {
                                        context.output(unparsedMessages, json);
                                    }

                                }
                            })
                            .withOutputTags(parsedMessages, TupleTagList.of(unparsedMessages)));


        }
    }




    public static void main(String[] args) {


        String inputTopic = "projects/nttdata-c4e-bde/topics/uc1-input-topic-3";
        String dlqTopic = "projects/nttdata-c4e-bde/topics/uc1-dlq-topic-3";
        String outputTable = "uc1_3.account";

        // direct runner on local
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        options.setStreaming(true);
        options.setRunner(DirectRunner.class);
        options.setInputTopic(inputTopic);
        options.setPubsubRootUrl("http://127.0.0.1:8085");

        Pipeline pipeline = Pipeline.create(options);

//        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
//        coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);

        // Build the table schema for the output table.
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("id").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("surname").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);

        LOG.info("Building pipeline...");

        PCollectionTuple transformOut =
                pipeline.apply("ReadMessageFromPubSub", PubsubIO.readStrings()
                        .fromTopic(options.getInputTopic()))

//                 print raw messages
//                .apply("Print", ParDo.of(new DoFn<String, String>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext c) {
//                        String line = c.element();
//                        System.out.println(line);
//                        }
//                    }));
                .apply("ConvertMessageToAccount", new PubsubMessageToAccount());




        // Write parsed messages to BigQuery
        transformOut
                // Retrieve parsed messages
                .get(parsedMessages)
                // Print field to screen
                .apply("Print", ParDo.of(new DoFn<Account, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Account account = c.element();
                        if (account != null) {
                            System.out.println("message body: id->" + account.id + " name->" + account.name + " surname->" + account.surname);
                        }
                        }
                    }));
                // convert to tablerow then write to BigQuery
//                .apply("ToBQRow", ParDo.of(new DoFn<Account, TableRow>() {
//                    @ProcessElement
//                    public void processElement(ProcessContext c) throws Exception {
//                        TableRow row = new TableRow();
//                        Account account = c.element();
//                        assert account != null;
//                        row.set("id", account.getId());
//                        row.set("name", account.getName());
//                        row.set("surname", account.getSurname());
//                        c.output(row);;
//                    }
//                }))
//                .apply(BigQueryIO.writeTableRows().to(outputTable)
//                .withSchema(schema)
//                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));



        // Write unparsed messages to Cloud Storage
        transformOut
                // Retrieve unparsed messages
                .get(unparsedMessages)
                .apply("WriteToPubSub", PubsubIO.writeStrings().to(dlqTopic));



        pipeline.run().waitUntilFinish();

    }

}
