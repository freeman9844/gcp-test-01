package com.example.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.CharacterMaskConfig;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InfoTypeTransformations;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DlpMaskingFn extends DoFn<KV<String, Iterable<TableRow>>, TableRow> {
    private static final Logger LOG = LoggerFactory.getLogger(DlpMaskingFn.class);

    private final Counter rowsProcessed = Metrics.counter(DlpMaskingFn.class, "rowsProcessed");
    private final Counter rowsFailed = Metrics.counter(DlpMaskingFn.class, "rowsFailed");
    private final Counter dlpApiCalls = Metrics.counter(DlpMaskingFn.class, "dlpApiCalls");

    private final String projectId;
    private final String deidentifyTemplateName;
    private final String inspectTemplateName;

    // Tags for multiple outputs
    private final TupleTag<TableRow> successTag;
    private final TupleTag<TableRow> deadLetterTag;

    private transient DlpServiceClient dlpServiceClient;

    public DlpMaskingFn(String projectId, String deidentifyTemplateName, String inspectTemplateName,
            TupleTag<TableRow> successTag, TupleTag<TableRow> deadLetterTag) {
        this.projectId = projectId;
        this.deidentifyTemplateName = deidentifyTemplateName;
        this.inspectTemplateName = inspectTemplateName;
        this.successTag = successTag;
        this.deadLetterTag = deadLetterTag;
    }

    @Setup
    public void setup() throws IOException {
        dlpServiceClient = DlpServiceClient.create();
    }

    @Teardown
    public void teardown() {
        if (dlpServiceClient != null) {
            dlpServiceClient.close();
        }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Iterable<TableRow> rows = c.element().getValue();
        List<TableRow> rowList = new ArrayList<>();
        rows.forEach(rowList::add);

        if (rowList.isEmpty()) {
            return;
        }

        try {
            // ---------------------------------------------------------
            // 1. Convert Beam TableRows to DLP Table
            // ---------------------------------------------------------
            // We assume all rows in a batch have the same schema.
            // Using a TreeSet ensures deterministic header ordering, which is crucial for
            // DLP handling.
            Set<String> headers = new java.util.TreeSet<>();
            for (TableRow row : rowList) {
                headers.addAll(row.keySet());
            }
            List<FieldId> dlpHeaders = headers.stream()
                    .map(h -> FieldId.newBuilder().setName(h).build())
                    .collect(Collectors.toList());

            // Build DLP Rows from TableRows
            List<Table.Row> dlpRows = new ArrayList<>();
            for (TableRow row : rowList) {
                Table.Row.Builder dlpRowBuilder = Table.Row.newBuilder();
                for (String header : headers) {
                    Object val = row.get(header);
                    Value.Builder valueBuilder = Value.newBuilder();
                    if (val == null) {
                        // DLP Table requires a value for every cell. Use empty string for nulls.
                        valueBuilder.setStringValue("");
                    } else {
                        valueBuilder.setStringValue(val.toString());
                    }
                    dlpRowBuilder.addValues(valueBuilder.build());
                }
                dlpRows.add(dlpRowBuilder.build());
            }

            // Create the ContentItem with the Table
            Table dlpTable = Table.newBuilder()
                    .addAllHeaders(dlpHeaders)
                    .addAllRows(dlpRows)
                    .build();

            ContentItem contentItem = ContentItem.newBuilder().setTable(dlpTable).build();

            // ---------------------------------------------------------
            // 2. Prepare DLP De-identify Request
            // ---------------------------------------------------------
            DeidentifyContentRequest.Builder requestBuilder = DeidentifyContentRequest.newBuilder()
                    .setParent("projects/" + projectId)
                    .setItem(contentItem);

            // Apply Configuration: Use Template if provided, otherwise use Inline Config
            if (deidentifyTemplateName != null && !deidentifyTemplateName.isEmpty()) {
                requestBuilder.setDeidentifyTemplateName(deidentifyTemplateName);
            } else {
                // -----------------------------------------------------
                // Inline De-identification Configuration
                // -----------------------------------------------------

                // Define InfoTypes to detect
                InfoType rrnInfoType = InfoType.newBuilder().setName("KOREA_RRN").build();
                InfoType phoneInfoType = InfoType.newBuilder().setName("PHONE_NUMBER").build();

                // Rule 1: Mask Korean RRN (mask last 7 digits)
                // e.g., 123456-1234567 -> 123456-*******
                CharacterMaskConfig rrnMaskConfig = CharacterMaskConfig.newBuilder()
                        .setMaskingCharacter("*")
                        .setNumberToMask(7)
                        .setReverseOrder(true)
                        .build();
                PrimitiveTransformation rrnPrimitive = PrimitiveTransformation.newBuilder()
                        .setCharacterMaskConfig(rrnMaskConfig)
                        .build();
                InfoTypeTransformations.InfoTypeTransformation rrnTransformation = InfoTypeTransformations.InfoTypeTransformation
                        .newBuilder()
                        .addInfoTypes(rrnInfoType)
                        .setPrimitiveTransformation(rrnPrimitive)
                        .build();

                // Rule 2: Mask Phone Number (mask last 4 digits)
                // e.g., 010-1234-5678 -> 010-1234-****
                CharacterMaskConfig phoneMaskConfig = CharacterMaskConfig.newBuilder()
                        .setMaskingCharacter("*")
                        .setNumberToMask(4)
                        .setReverseOrder(true)
                        .build();

                PrimitiveTransformation phonePrimitive = PrimitiveTransformation.newBuilder()
                        .setCharacterMaskConfig(phoneMaskConfig)
                        .build();

                InfoTypeTransformations.InfoTypeTransformation phoneTransformation = InfoTypeTransformations.InfoTypeTransformation
                        .newBuilder()
                        .addInfoTypes(phoneInfoType)
                        .setPrimitiveTransformation(phonePrimitive)
                        .build();

                // Combine Transformations
                InfoTypeTransformations transformations = InfoTypeTransformations.newBuilder()
                        .addTransformations(rrnTransformation)
                        .addTransformations(phoneTransformation)
                        .build();

                DeidentifyConfig deidentifyConfig = DeidentifyConfig.newBuilder()
                        .setInfoTypeTransformations(transformations)
                        .build();
                requestBuilder.setDeidentifyConfig(deidentifyConfig);
            }

            // Apply Inspection Config (Required for inline rules or if using
            // InspectTemplate)
            if (inspectTemplateName != null && !inspectTemplateName.isEmpty()) {
                requestBuilder.setInspectTemplateName(inspectTemplateName);
            } else if (deidentifyTemplateName == null || deidentifyTemplateName.isEmpty()) {
                // If using inline de-id, we need to specify what InfoTypes to inspect for.
                InfoType rrnInfoType = InfoType.newBuilder().setName("KOREA_RRN").build();
                InfoType phoneInfoType = InfoType.newBuilder().setName("PHONE_NUMBER").build();

                InspectConfig inspectConfig = InspectConfig.newBuilder()
                        .addInfoTypes(rrnInfoType)
                        .addInfoTypes(phoneInfoType)
                        // Lower threshold to 'POSSIBLE' to ensure we catch RRNs even in ambiguous
                        // contexts
                        .setMinLikelihood(com.google.privacy.dlp.v2.Likelihood.POSSIBLE)
                        .build();
                requestBuilder.setInspectConfig(inspectConfig);
            }

            // ---------------------------------------------------------
            // 3. Call DLP API
            // ---------------------------------------------------------
            DeidentifyContentResponse response = deidentifyContent(requestBuilder.build());
            dlpApiCalls.inc(); // Metrics: Track API calls
            Table deidentifiedTable = response.getItem().getTable();

            // ---------------------------------------------------------
            // 4. Convert DLP Result back to TableRows
            // ---------------------------------------------------------
            List<Table.Row> outputRows = deidentifiedTable.getRowsList();
            for (Table.Row outputRow : outputRows) {
                TableRow tableRow = new TableRow();
                for (int i = 0; i < dlpHeaders.size(); i++) {
                    String header = dlpHeaders.get(i).getName();
                    Value value = outputRow.getValues(i);
                    // Convert DLP Value back to String or appropriate type
                    if (value.hasIntegerValue()) {
                        tableRow.set(header, value.getIntegerValue());
                    } else if (value.hasFloatValue()) {
                        tableRow.set(header, value.getFloatValue());
                    } else if (value.hasBooleanValue()) {
                        tableRow.set(header, value.getBooleanValue());
                    } else if (value.hasTimestampValue()) {
                        tableRow.set(header, value.getTimestampValue().toString());
                    } else if (value.hasDateValue()) {
                        tableRow.set(header, value.getDateValue().toString());
                    } else if (value.hasTimeValue()) {
                        tableRow.set(header, value.getTimeValue().toString());
                    } else {
                        tableRow.set(header, value.getStringValue());
                    }
                }
                // Output successful row
                c.output(successTag, tableRow);
                rowsProcessed.inc(); // Metrics: Track success
            }

        } catch (Exception e) {
            // ---------------------------------------------------------
            // 5. Error Handling (Dead Letter Queue)
            // ---------------------------------------------------------
            LOG.error("Error processing batch", e);
            rowsFailed.inc(rowList.size()); // Metrics: Track failures

            // If a batch fails, send ALL rows in that batch to the Dead Letter Queue (Error
            // Table)
            // This prevents data loss and allows for later replay/analysis.
            for (TableRow row : rowList) {
                c.output(deadLetterTag, row);
            }
        }
    }

    protected DeidentifyContentResponse deidentifyContent(DeidentifyContentRequest request) {
        return dlpServiceClient.deidentifyContent(request);
    }
}
