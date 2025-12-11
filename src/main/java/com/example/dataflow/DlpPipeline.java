package com.example.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;

public class DlpPipeline {

    public interface DlpPipelineOptions extends DataflowPipelineOptions {
        @Description("Input BigQuery table spec (project:dataset.table)")
        @Required
        String getInputTable();

        void setInputTable(String value);

        @Description("Output BigQuery table spec (project:dataset.table)")
        @Required
        String getOutputTable();

        void setOutputTable(String value);

        @Description("DLP De-identify Template Name (full resource path) - Optional. If not provided, defaults to masking KOREA_RRN.")
        String getDeidentifyTemplateName();

        void setDeidentifyTemplateName(String value);

        @Description("DLP Inspect Template Name (full resource path) - Optional")
        String getInspectTemplateName();

        void setInspectTemplateName(String value);

        @Description("Batch size for DLP API calls")
        Integer getBatchSize();

        void setBatchSize(Integer value);
    }

    public static void main(String[] args) {
        DlpPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(DlpPipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        // Default batch size if not provided
        long batchSize = options.getBatchSize() != null ? options.getBatchSize() : 100L;

        // Define output tags for success and failure
        TupleTag<TableRow> successTag = new TupleTag<TableRow>() {
        };
        TupleTag<TableRow> deadLetterTag = new TupleTag<TableRow>() {
        };

        PCollectionTuple outputTuple = p.apply("ReadFromBigQuery", BigQueryIO.readTableRows()
                .from(options.getInputTable())
                .withMethod(Method.DIRECT_READ)) // Use Storage Read API
                .apply("AddKeys", WithKeys.of(new SimpleFunction<TableRow, String>() {
                    @Override
                    public String apply(TableRow input) {
                        // Random key to distribute load for batching
                        return Integer.toString(ThreadLocalRandom.current().nextInt(100));
                    }
                }))
                .apply("BatchRows", GroupIntoBatches.ofSize(batchSize))
                .apply("DlpMasking", ParDo.of(new DlpMaskingFn(
                        options.getProject(),
                        options.getDeidentifyTemplateName(),
                        options.getInspectTemplateName(),
                        successTag,
                        deadLetterTag))
                        .withOutputTags(successTag, TupleTagList.of(deadLetterTag)));

        // Write Successful Rows
        outputTuple.get(successTag)
                .apply("WriteToBigQuery", BigQueryIO.writeTableRows()
                        .to(options.getOutputTable())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER) // Assume table exists
                                                                                                // or change to
                                                                                                // CREATE_IF_NEEDED
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)); // Use Storage Write API

        // Write Failed Rows (Dead Letter Queue)
        // We write to a table named {output_table}_error
        String errorTable = options.getOutputTable() + "_error";
        outputTuple.get(deadLetterTag)
                .apply("WriteToDLQ", BigQueryIO.writeTableRows()
                        .to(errorTable)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API));

        p.run();
    }
}
